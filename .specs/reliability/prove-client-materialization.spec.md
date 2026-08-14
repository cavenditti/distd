---
id: TASK:reliability/prove-client-materialization
type: task
status: accepted
summary: >
  Close the product integrity and destination guarantees at the client storage
  boundary with explicit authority projection and corruption regressions.
owners: [carlo]
progress: done
refines:
  - REQ:reliability/verified-client-materialization#c-received
  - REQ:reliability/verified-client-materialization#c-reused
  - REQ:reliability/verified-client-materialization#c-root
  - REQ:reliability/verified-client-materialization#c-destination
  - REQ:reliability/verified-client-materialization#c-failure
assignee: carlo
eta:
blocked_by: []
aspects: [integrity, containment, activation, evidence]
---

# Prove client materialization

## Plan

Project authenticated remote metadata into a private activation value that
contains only the display name, revision, and description before calling the
storage worker. Move the complete remote metadata into manifest verification,
and pass the caller-selected destination separately so the remote deployment
hint is unrepresentable at the storage boundary.

Retain received-chunk hash validation, bounded reusable-backing copy and hash,
complete root reconstruction, active-reference seals, and contained activation
paths. Add regressions proving a corrupt reusable ordinary backing is rejected
without selecting a destination and a hostile remote deployment hint cannot
influence the caller-selected destination. Preserve the prior received-byte,
symlink, traversal, old-handle, fault/restart, and immutable-seal coverage.

## Acceptance

- A received chunk with the wrong hash and an ordinary reused backing whose
  bytes changed without changing length both fail before destination selection.
- Remote metadata with an absolute or parent-traversing deployment hint can
  contribute no destination value to the storage call; only the explicit local
  destination is activated beneath the storage root.
- Exact valid cold and delta activation behavior, sealed immutable reuse, old
  handles, cache bounds, and crash recovery remain unchanged.
- Focused client/core tests, formatting, strict configured Clippy, spec lint,
  and the locked/offline workspace suite pass on the final source.
