---
id: TASK:storage/crash-consistent-activation
type: task
status: accepted
summary: >
  Make client activation observably atomic and recoverable across process or
  machine interruption at every filesystem transition.
owners: [carlo]
progress: pending
refines:
  - REQ:storage/durable-artifact-state#c-activation-observation
  - REQ:storage/durable-artifact-state#c-activation-recovery
  - REQ:storage/durable-artifact-state#c-durability
  - REQ:reliability/durable-state-contract#c-envelope
  - REQ:reliability/durable-state-contract#c-migration
aspects: [observation, recovery, durability, envelope, migration]
assignee: carlo
eta:
blocked_by: [TASK:reliability/durable-state-envelopes]
---

# Crash consistent activation

## Plan

Replace the best-effort backup sequence in
[FsStorage activation](spec:src:core/src/chunk_storage/fs_storage.rs) with an
explicit same-filesystem activation transaction that stages each verified tree
under an immutable revision root. Commit by atomically replacing one active
root/name reference. Readers MUST enter through the storage API and open an
immutable revision handle; all paths resolved through that handle remain in the
same revision for its lifetime. Sync every staged file and required directory
before the switch, and recover transaction remnants before exposing an active
handle. Preserve manifest verification, containment, file/dir replacement, and
the previous revision on ordinary failure. Cross-filesystem destinations,
historical activation UX, and whole-tree guarantees for arbitrary direct path
walkers outside the API are non-goals.

## Acceptance

- Concurrent readers repeatedly opening an active revision handle see either
  the complete old tree or complete new tree. A handle opened before the atomic
  root/name switch continues resolving only the immutable old revision, and a
  handle opened after it resolves only the new revision; neither observes
  absence, mixed entries, truncation, staging names, or backup names.
- Deterministic fault injection after every create, write, file sync, directory
  sync, rename/switch, cleanup, and state-record operation proves restart
  recovers exactly one verified revision.
- Recovery is idempotent across repeated crashes and rejects/quarantines
  malformed transaction state without deleting the last known-good revision.
- Single-file, directory, file-to-directory, directory-to-file, and removed
  entry transitions retain containment and integrity guarantees. Direct
  filesystem readers outside the handle API are tested only for per-entry
  atomic replacement, not impossible whole-tree snapshot consistency.
- Focused crash/restart tests run on supported filesystem/platform paths and
  the full documented validation suite passes.
