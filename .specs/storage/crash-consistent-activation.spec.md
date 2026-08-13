---
id: TASK:storage/crash-consistent-activation
type: task
status: accepted
summary: >
  Make client activation observably atomic and recoverable across process or
  machine interruption at every filesystem transition.
owners: [carlo]
progress: done
refines:
  - REQ:storage/durable-artifact-state#c-activation-observation
  - REQ:storage/durable-artifact-state#c-activation-recovery
  - REQ:storage/durable-artifact-state#c-durability
  - REQ:reliability/durable-state-contract#c-envelope
  - REQ:reliability/durable-state-contract#c-migration
aspects: [observation, recovery, durability, envelope, migration]
assignee: carlo
eta:
blocked_by: []
---

# Crash consistent activation

## Plan

Replace the best-effort backup sequence in
[FsStorage activation](spec:src:core/src/chunk_storage/fs_storage.rs) with an
explicit same-filesystem transaction rooted at `.distd-activation-v1`.
Materialize every candidate as a uniform immutable revision tree, then commit
by atomically replacing one hidden active-reference envelope. The active
reference, not the compatibility destination path or the mutable FsStorage
index, is authoritative. `FsStorage` exposes an `ActiveRevisionHandle` that
pins one immutable revision and resolves every file through that revision for
the handle's lifetime. Existing receive methods remain compatibility wrappers
and the ordinary destination remains a materialized mirror for callers that do
not use the handle API.

Register durable kind `FsActivation = 4`. Schema 1 is the canonical active
reference and schema 2 is the canonical transaction record; each has a 64 MiB
hard payload ceiling and stable nonempty golden bytes. The reserved layout is
`revisions/<revision-digest>/root`, `active/<destination-digest>.state`,
`transactions/<transaction-id>.state`, `staging/<transaction-id>/root`, and
`quarantine/`. Destination and revision digests are domain-separated and bind
the canonical native destination bytes and exact validated Item/manifest/chunk
identity respectively. A fully validated current selector is authoritative for
its exact canonical destination and suppresses only that destination's stale
derived-checkpoint projection; malformed or ambiguous selector state fails
closed.

Existing direct-path FsStorage schema-1 and its declared raw legacy format stay
byte-compatible and are adopted lazily for a requested destination after the
old bytes and metadata verify; migration retains the source.

The current transaction order is: durably record an Intent that authenticates
all activation-owned paths; build and verify the staged tree; sync files and
changed directories bottom-up; publish the immutable revision and sync its
parent; atomically replace the active-reference envelope and sync its parent;
publish the matching in-memory selector; rebuild the derived in-memory
FsStorage projection; reconcile the compatibility mirror; then enqueue
cancellation-safe removal of the retained Intent and sync its parent. The
active-reference parent sync is the commit boundary. Schema-2 Ready and
Switched values remain accepted only for byte-compatible recovery of records
written by earlier implementations; the current writer emits Intent only. The
FsStorage envelope is a derived checkpoint and is not synchronously replaced
on the activation path. Before the commit boundary, an ordinary failure
preserves the old active reference and mirror. A failure after the reference
rename is indeterminate: existing handles remain valid, new opens and
activations fail closed until restart, and recovery chooses the complete
validated reference actually present rather than attempting an unsafe
rollback. Failure to enqueue deferred cleanup fails closed; a cleanup error
discovered by the owned worker poisons further opens and activations until
restart, while the retained authenticated Intent lets recovery converge.

Construction authenticates active-reference envelopes before decoding the
derived FsStorage checkpoint. A valid selector suppresses only the checkpoint
item, backing records, and exclusively reachable link subgraph claimed by that
canonical destination; unrelated checkpoint corruption remains fatal. Startup
reconstructs selected Items, immutable backing offsets, indexes, and complete
link graphs, reconciles mirrors and retained Intent or legacy Ready/Switched
records, durably repairs the derived checkpoint, and is idempotent. A
transaction selected by its reference finishes the selected state and mirror;
an unselected Intent or Ready preserves the selected prior revision and removes
only authenticated owned remnants. An unselected legacy Switched record is
contradictory and fails closed. Immutable revision reclamation remains deferred
so handles opened before a switch remain valid. Writers serialize per canonical
destination; different destinations may proceed independently.

The configured storage root and its already-validated parent hierarchy are a
single-process, non-hostile deployment boundary. Multi-process writers and
external mutation of those ancestors during an operation are out of scope.
Final-component symlinks, special files, malformed reserved entries, traversal,
and cross-filesystem staging are rejected before the switch. Whole-tree
atomicity is promised only through `ActiveRevisionHandle`: compatibility-mirror
file-to-directory or directory-to-file transitions may have transient absence,
while ordinary files are replaced atomically per entry.

## Acceptance

- Concurrent readers repeatedly opening an active revision handle see either
  the complete old tree or complete new tree. A handle opened before the atomic
  root/name switch continues resolving only the immutable old revision, and a
  handle opened after it resolves only the new revision; neither observes
  absence, mixed entries, truncation, staging names, or backup names.
- Deterministic fault injection after every current-writer and recovery create,
  write, file sync, directory sync, rename/switch, cleanup, and authoritative
  state-record operation proves restart
  recovers at most one verified active reference. An interrupted initial
  activation before the commit boundary may correctly recover none; an update
  recovers exactly the old or new active reference. Inactive verified revision
  trees may coexist and are not treated as multiple active revisions.
- Recovery is idempotent across repeated crashes and rejects/quarantines
  malformed transaction state without deleting the last known-good revision.
- Single-file, directory, file-to-directory, directory-to-file, and removed
  entry transitions retain containment and integrity guarantees. Direct
  filesystem readers outside the handle API are tested only for per-entry
  atomic replacement, not impossible whole-tree snapshot consistency.
- The fault/restart matrix covers initial activation and update; shorter,
  longer, empty, reused, received, and mixed chunks; nested entry add, remove,
  and rename; every shared envelope replacement stage used by current Intent
  and active-reference writes; every staging, revision, mirror,
  deferred-cleanup, recovery-cleanup, and derived-checkpoint repair boundary;
  recovery twice after each failure; final-component symlink/special-file
  conflicts; and malformed, wrong-kind, future, truncated, trailing, oversized,
  and noncanonical records. Frozen schema-2 fixtures separately prove legacy
  Ready and Switched decoding and recovery without making those phases part of
  the current writer protocol.
- Continuous readers prove an old handle remains wholly old while new handles
  are wholly old or new. Same-destination candidates serialize to one monotonic
  active result, different destinations remain independent, and cancellation
  cannot abandon an owned activation coordinator.
- Reference budgets use two independent campaigns of 20 fresh-root updates per
  activation shape and three independent component campaigns. Each component
  campaign performs 20 active-reference replacements, 1,000 handle opens, 100
  direct reads, 100 sets of 64 indexed chunk reads, and five restarts. Each gate
  uses the worst campaign p95; failed campaigns remain evidence rather than
  being replaced by favorable reruns. The single-file, directory,
  active-reference, and indexed-read ceilings are calibrated once as
  `ceil-to-10-ms(1.20 * worst-campaign-p95)` and are not retuned again during
  the task. Handle-open, direct-read, and restart ceilings retain their earlier
  reference budgets because those already had material headroom. On the
  measured Apple M4 Pro, macOS
  26.5.2 APFS reference system, a 16 MiB Fixed256K update with one changed chunk
  must complete single-file activation within 100 ms p95 and a 64-file
  directory activation within 200 ms p95. The durable active-reference switch
  must remain within 30 ms p95; opening an immutable active handle remains
  within 1 ms p95 without artifact copying; a full direct read remains within
  10 ms and 64 indexed chunk reads within 50 ms. Recovery/reopen remains within
  120 ms without a full artifact rehash. These are reproducible local
  regression budgets with rounded headroom over the preserved worst campaigns,
  not portable product SLOs. Filesystem work, hashing, encoding, syncing,
  switching, and recovery run outside Tokio workers; code-path inspection and
  cancellation tests prove async coordination holds contain no filesystem work
  and remain within the 1 ms structural budget.
  After successful activation, the net unique-inode APFS allocated-block delta
  is at most 1.10 times the bytes of changed whole entries plus 1 MiB of
  metadata, and unchanged directory entries are reused rather than copied into
  a second complete tree. This is an allocated-footprint gate, not a claim about
  physical device bytes written. Activation-owned write/read buffers remain at
  or below 8 MiB independent of entry count: immutable reads MUST NOT eagerly
  allocate the current 2 MiB `BufWriter` capacity for every directory entry,
  and the measured update supplies only its changed 256 KiB chunk while reused
  chunks stream from the prior revision through the bounded 1 MiB copy buffer.
- Focused crash/restart tests run on supported filesystem/platform paths and
  the full documented validation suite passes.

## Verification evidence

The pre-proof production snapshot
`9bc8079f46049c8a1ca82e6a0e5b2944aa9d0e205b3e5f6b113b2bc97a1a6f71`
was measured twice for activation and three times for components before the
local ceilings were frozen. Worst campaign p95s were 79.529 ms single-file,
164.725 ms directory, 20.917 ms active-reference replacement, 0.833 us handle
open, 1.708 ms direct read, 41.184 ms for 64 indexed reads, and 42.127 ms
restart. The observed allocation deltas were 16,785,408 bytes for a single-file
update and 274,432 bytes for a directory update. The frozen 20%-plus-rounded
rule yields the 100/200/30/50 ms recalibrated ceilings above. The 1/10/120 ms
handle/direct/restart ceilings are retained prior budgets rather than products
of that formula; favorable later measurements do not reduce or redefine any of
the gates.

The checked-in ignored release test
`chunk_storage::fs_storage::tests::activation_reference_performance` preserves
the generator, cache limits, fresh-root campaign shape, percentile rule, raw
sample output, semantic/restart checks, allocation accounting, and every
measured latency or allocation ceiling. Async isolation is proven separately by
the production `spawn_blocking` route and cancellation tests. On the final
verification run its worst campaign p95s were 43.445 ms single-file, 73.807 ms
directory, 9.056 ms active-reference replacement, 1.791 us handle open,
2.422 ms direct read, 4.901 ms for 64 indexed reads, and 17.870 ms restart.
Maximum allocated-byte deltas were 16,785,408 bytes and 270,336 bytes
respectively. The test ran in release mode on Apple M4 Pro,
macOS 26.5.2, arm64, APFS-backed `/private/tmp`; these results are local
regression evidence rather than cross-platform latency guarantees.
