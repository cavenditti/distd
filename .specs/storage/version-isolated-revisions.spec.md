---
id: TASK:storage/version-isolated-revisions
type: task
status: accepted
summary: >
  Store server artifact revisions at immutable version-isolated locations so
  later publication cannot overwrite historical bytes.
owners: [carlo]
progress: pending
refines: [REQ:storage/durable-artifact-state#c-version-isolation]
assignee: carlo
eta:
blocked_by: [TASK:reliability/durable-state-envelopes]
---

# Version isolated revisions

## Plan

Separate immutable server storage identity from `Item.metadata.path`. Replace
the path-reusing behavior in [FsStorage item placement](spec:src:core/src/chunk_storage/fs_storage.rs)
with a persistent CAS or a deterministic artifact-ID/version namespace while
retaining chunk deduplication. Migrate declared-supported state using the
independent durable-state envelope rather than silently rebuilding it.
Client-selected mutable destinations, garbage collection, retention policy, and changing the
artifact/version API are non-goals.

## Acceptance

- Publishing two different single-file revisions with the same artifact ID and
  metadata path leaves both exact versions fetchable with their original root
  and bytes before and after server restart.
- The same property holds for directories when entries are changed, renamed,
  or removed, and for deduplicated chunks shared by revisions.
- Re-publication remains idempotent and does not allocate a new version only
  when both content and every version-relevant metadata field are identical.
  Identical bytes with changed version-relevant metadata MUST NOT be collapsed;
  concurrent publication cannot alias two versions to mutable path storage.
- A migration fixture for every supported prior storage envelope preserves all
  exact-version reads; unsupported state fails explicitly without overwriting
  its source file.
- Focused filesystem and server sync tests plus the full documented validation
  suite pass.
