---
id: TASK:storage/durable-publication-commit
type: task
status: accepted
summary: >
  Publish server metadata only after revision storage and its durable metadata
  snapshot commit successfully as one recoverable transition.
owners: [carlo]
progress: done
refines:
  - REQ:storage/durable-artifact-state#c-publication-commit
  - REQ:storage/durable-artifact-state#c-durability
  - REQ:reliability/durable-state-contract#c-envelope
  - REQ:reliability/durable-state-contract#c-canonical
  - REQ:reliability/durable-state-contract#c-migration
aspects: [publication, durability, envelope, canonicalization, migration]
assignee: carlo
eta:
blocked_by: []
---

# Durable publication commit

## Plan

Refactor [single-file and directory publication](spec:src:server/src/server.rs)
into an owned coordinator task that survives cancellation of the request which
awaits it. The coordinator builds an isolated candidate, retains the
per-artifact lock through a durable version-isolated storage commit, commits a
fresh global metadata snapshot, and only then swaps the in-memory state visible
to fetch and sync readers. Preparation, storage I/O, and metadata I/O run on
blocking workers rather than Tokio executor threads. Aborted candidates are
reconciled into unreferenced content-addressed chunks; an artifact/version Item
that was not published MUST NOT remain authoritative after recovery.

Register durable-state kind `ServerPublication = 3`, payload schema 1, with a
64 MiB payload cap, at most 65,536 feeds, 65,536 artifacts, and 1,048,576 total
revisions. The current path is `metadata.state`; the distinct legacy path
remains `metadata.bitcode`. Current presence suppresses all legacy fallback.
The canonical disk DTO contains the server version, feeds as a strictly sorted
vector, and one strictly sorted artifact-history vector whose revisions are
strictly increasing; latest items are derived from the final revisions rather
than stored redundantly. Feed keys/names and all nested Item identities, roots,
chunks, and directory paths are validated. Logically equal runtime HashMaps
therefore produce identical payload and envelope bytes.

Declare only the exact raw-Bitcode three-field `{version, feeds, items}` and
four-field `{version, feeds, items, versions}` layouts supported for legacy
migration. Migration is bounded, semantically validated, backend-verified, and
written to the distinct current path; the legacy source remains byte-exact.
When both paths are absent, durably bootstrap an empty current envelope before
listeners start. Startup validates every retained revision against the selected
filesystem or redb backend and removes only unreferenced server-revision index
records before exposing any reader.

The metadata writer uses unique same-directory staging and the shared durable
replacement primitive. A pre-rename failure preserves the prior disk and
memory snapshot. A post-rename failure is `CommitIndeterminate`: reconcile the
complete target, keep the prior in-memory snapshot hidden, enter a fail-closed
read-only publication state, and require restart; crash recovery may select the
complete previous or complete new snapshot. It is never reported as an
ordinary failure with a guaranteed old directory entry. Multi-process writers,
retention, and content-addressed orphan garbage collection are non-goals.

## Acceptance

- Injected serialization, staging-create, partial-write, and file-sync failures
  return a pre-commit failure while latest, exact-version, REST metadata, and
  on-disk metadata retain the previous revision. Post-rename or directory-sync
  failure returns an explicit indeterminate outcome, keeps live readers on the
  prior snapshot, and blocks further mutation until restart.
- Crash injection at every storage/metadata/visibility boundary restarts into
  either the complete previous revision or complete new revision; metadata
  never references missing or mismatched bytes.
- Concurrent fetch/sync readers cannot observe the candidate until durable
  commit succeeds. Cancelling or losing the caller response cannot cancel the
  coordinator, release its artifact lock early, or permit revision reuse;
  successful and lost-response retries remain idempotent and monotonic.
- Logically equal metadata assembled with different map/set insertion orders
  produces identical envelope bytes; corrupt or unsupported legacy metadata is
  preserved and fails explicitly rather than becoming an empty server.
- Current state rejects symlink/special/wrong-kind/future/truncated/trailing or
  oversized files without legacy fallback. Exact legacy fixtures for both
  supported layouts migrate with source retention, while ambiguous or corrupt
  legacy input creates no current file.
- Startup rejects any metadata revision whose ordered chunks, sizes, hashes,
  root, manifest, or directory entries disagree with the selected backend.
  Aborted storage work may leave immutable chunks but no conflicting retained
  artifact/version authority after reconciliation.
- Single-file and directory tests exercise both filesystem and redb/default
  server backends where supported. Fault tests cover every shared replacement
  stage, coordinator cancellation, same/different-artifact concurrency,
  continuous readers, retry/restart, and metadata visibility, followed by the
  full validation suite.
- On the measured local APFS reference, a canonical metadata snapshot of at
  most 4 MiB targets atomic replacement p95 at or below 15 ms and end-to-end
  metadata-tail p95 at or below 30 ms for 1,000 retained revisions. At 10,000
  revisions the payload has a 48 MiB soft ceiling (64 MiB hard) and the tail
  targets p95 at or below 120 ms. Clone, canonicalization, codec, integrity,
  write, fsync, and rename work stays off Tokio executor threads; the async
  coordination lock itself targets p95 at or below 1 ms.
