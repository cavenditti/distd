---
id: TASK:storage/version-isolated-revisions
type: task
status: accepted
summary: >
  Store server artifact revisions at immutable version-isolated locations so
  later publication cannot overwrite historical bytes.
owners: [carlo]
progress: done
refines: [REQ:storage/durable-artifact-state#c-version-isolation]
assignee: carlo
eta:
blocked_by: []
---

# Version isolated revisions

## Plan

Separate immutable server storage identity from `Item.metadata.path` without
changing the public artifact/version or client-destination API. Server
publication passes the stable artifact ID into storage before any item is
indexed. Filesystem-backed server revisions use a reserved, generated ASCII
chunk-addressed namespace. Each unique verified chunk has one immutable
backing file while the durable item manifest binds the ordered chunks, logical
metadata, and directory entries into a version. A later low-delta revision
therefore writes only newly unique chunks rather than copying the full artifact.
The logical metadata path remains only a deployment hint. Reusing an existing
object is allowed only after its complete bytes and shape match; a conflict,
symbolic link, or special file fails closed. HashMap and redb backends retain
their chunk CAS but reject a supplied hash or child relation that disagrees
with an existing key.

Choose the next revision with checked arithmetic under a per-artifact
publication lock; preparing and hashing a candidate happens before that lock,
and the synchronous storage commit runs on a blocking worker rather than a
Tokio executor thread. Determine idempotence before committing a new storage
item: equality covers stable artifact ID, display name, logical path,
description, root, ordered directory entries, chunks, and chunk algorithm.
Identical bytes with a changed version-relevant metadata field therefore create
a new version while sharing immutable backing bytes. A true duplicate returns
the previous item without indexing or persisting a speculative next-version
item. Metadata-file replacement remains serialized across artifacts; its
durability protocol is owned by the separately blocked durable-publication
task.

The supported schema-1 FsStorage envelope and the frozen pre-envelope format
are migrated by verifying every populated chunk's size and hash, reconstructing
each retained item, materializing immutable backing objects, and durably
rewriting equivalent canonical state. A supported envelope is atomically
rewritten only after the new backing objects are complete; the separate frozen
pre-envelope source remains retained. Any overwritten alias or ambiguous or
unverifiable item fails explicitly without changing the source bytes.
Current-envelope presence suppresses legacy fallback. Client-selected mutable
destinations, garbage collection, retention policy, multi-process writers, and
changing the artifact/version API are non-goals.

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
- Different artifacts may share one logical metadata path without sharing a
  mutable backing path; revision exhaustion fails explicitly rather than
  saturating or replacing an existing version.
- Directory entry paths are unique, canonical relative UTF-8 paths. Absolute,
  parent-traversing, non-UTF-8, duplicate, symbolic-link, special-file, or
  reserved-namespace collisions fail before an item becomes retrievable.
- A migration fixture for every supported prior storage envelope preserves all
  recoverable exact-version reads and retains its source bytes; overwritten,
  ambiguous, corrupt, or unsupported state fails explicitly without
  overwriting its source file.
- Filesystem and redb/hashmap CAS insertion verifies the supplied content hash
  and treats an existing mismatched value or child relation as an immutable-key
  conflict.
- For the 16 MiB, 64-chunk reference workload with exactly one changed 256 KiB
  chunk, filesystem and redb publication add no more than 1.10 times the newly
  unique chunk bytes plus 1 MiB of metadata/database overhead. Filesystem
  preparation does not add a separate whole-artifact content hash beyond its
  chunk verification, and exact chunk lookup is constant in the artifact's
  chunk count.
- Focused filesystem and server sync tests plus the full documented validation
  suite pass.
