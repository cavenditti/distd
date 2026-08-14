---
id: REQ:storage/durable-artifact-state
type: requirement
status: accepted
level: MUST
summary: >
  Immutable server revisions, client activation, and published metadata remain
  mutually consistent across failure, restart, and concurrent observation.
owners: [carlo]
refines:
  - REQ:product/artifact-delivery#c-identity
  - REQ:product/artifact-delivery#c-activation
aspects: [revision-identity, activation]
categorized_under: []
---

# Durable artifact state

## Context

`FsStorage` currently maps an item directly to its metadata path, so a new
server revision can overwrite bytes referenced by an older immutable version.
Client activation renames the old destination aside before installing the new
one and has incomplete crash recovery/fsync coverage. Separately, server
publication mutates visible in-memory metadata before persisting it. The
relevant boundaries are [filesystem storage](spec:src:core/src/chunk_storage/fs_storage.rs)
and [server publication](spec:src:server/src/server.rs).

:::{requirement id="durable-artifact-state" level="MUST"}
- {#c-version-isolation} Every accepted server revision MUST retain storage
  references to its own verified bytes; publishing a later revision, including
  one with the same metadata path, MUST NOT change exact-version retrieval.
- {#c-activation-observation} Client activation MUST stage an immutable verified
  revision and atomically switch one active root/name reference. A reader that
  opens the active revision through the storage API MUST resolve every entry
  through one immutable handle for its lifetime, so it observes the complete
  old or new tree rather than a mixture. Arbitrary direct per-file reads outside
  that API receive only the host filesystem's per-entry atomicity.
- {#c-activation-recovery} After interruption at any activation write, rename,
  or sync boundary, restart MUST deterministically recover the previous or new
  complete revision and remove or quarantine stale transaction artifacts.
- {#c-activation-locks} Per-destination activation serialization MUST retain a
  coordination slot only while an active or waiting operation references that
  exact canonical destination. Completion, rejection, and abandoned waiters
  MUST release their participant leases, and the last participant MUST remove
  the slot without weakening same-destination serialization.
- {#c-activation-count} Runtime activation MUST enforce the same 65,536 active
  destination ceiling as startup recovery. Concurrent first activations MUST
  reserve capacity atomically before creating activation-owned state; updates
  to an already active destination MUST remain admissible at the ceiling, and
  failed pre-selector reservations MUST release capacity.
- {#c-immutable-reader-cache} Immutable revision reader caching MUST retain at
  most one recency record per cached path. A successful hit MUST make that path
  most-recently used, eviction MUST remove the true least-recently used path,
  and repeated reads MUST NOT grow cache metadata.
- {#c-publication-commit} A server revision MUST NOT become visible through
  latest lookup, exact-version lookup, or metadata fetch until all referenced
  storage and metadata are durably committed; a failed commit MUST preserve the
  prior visible and durable state.
- {#c-durability} Durable commits MUST sync written files and the directories
  whose entries change in an order justified for the supported filesystems.
:::

## Non-goals

This requirement does not promise atomic replacement across filesystems,
distributed transactions across multiple server processes, historical client
destinations, or automatic rollback policy. Unreferenced data from an aborted
server publication may be reclaimed later, but it must never become visible.
:::
