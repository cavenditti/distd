---
id: TASK:reliability/durable-state-envelopes
type: task
status: accepted
summary: >
  Add the minimal canonical durable envelope and atomic migration primitive,
  then apply it to filesystem-storage state as the first consumer.
owners: [carlo]
progress: done
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

## Frozen format and migration contract

Envelope version 1 is a fixed 56-byte header followed by the payload. Bytes
0..8 are the ASCII magic `DISTDSTA`; bytes 8..10 are the little-endian kind
(`1` is filesystem storage); bytes 10..12 are envelope version `1`; bytes
12..14 are the little-endian payload schema; bytes 14..16 are zero flags;
bytes 16..24 are the little-endian `u64` payload length; and bytes 24..56 are
the BLAKE3 digest of the domain `distd durable state envelope v1\0`, header
bytes 0..24, and the payload. Filesystem-storage schema 1 has a hard 64 MiB
payload ceiling. Length, kind, envelope version, schema, flags, exact document
extent, and integrity are validated before Bitcode sees the payload.

Filesystem-storage schema 1 is a flat DTO. Runtime authority is not persisted:
the persistence path is constructor-owned, the root is bound by a
domain-separated digest of its canonical native path bytes, and chunk paths
are stored relative to that root. Sets and maps are encoded as vectors sorted
by the lexicographic Bitcode identity of their complete elements or entries;
duplicate identities are rejected. Runtime indexes, dirty/generation state,
locks, and atomics are reconstructed only after bounded decoding and semantic
validation.

The only supported legacy input is raw Bitcode 0.6.3 for the five serialized
fields of `FsStorageState` at source commit
`a5d7c9977f8f4bfa32ecdfda6c4c341578d52273`, in field order `root`, `items`,
`persistance_path`, `data`, and `links`. Legacy input is considered only when
the new digest-named envelope is absent. It is loaded through a frozen legacy
DTO, rebound to the constructor root, semantically validated, migrated to the
distinct envelope path, reread, and compared before success. The legacy file
is retained after migration; corrupt current envelopes never fall back to it.
The checked-in non-empty legacy byte fixture has SHA-256
`e0b7a94362b275ad5aac07fdef2b7e991c360e6041764781feb55b5c416a31f0`.

Atomic replacement uses a unique exclusive same-directory mode-0600 staging
file, complete write, file `sync_all`, rename, and parent-directory `sync_all`.
Pre-rename failure leaves the previous target untouched. Failure of directory
sync after rename is reported as an indeterminate commit; the caller must
reload rather than assume either version. The single-writer/non-hostile-parent
boundary matches the parent requirement's explicit multi-process non-goal.

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
- Corrupt or unreadable persistence is returned by fallible constructors; only
  an absent state file initializes an empty store. Decoded roots and paths
  cannot redirect handles or persistence outside constructor-owned locations.
- On representative local APFS hardware, a steady durable commit up to 1 MiB
  remains within a 15 ms p95 reference budget, while the complete canonical
  snapshot plus encode remains within 50 ms and decode plus filesystem-backed
  semantic validation remains within 120 ms at 1,000 items / 4,000 chunk
  records. Both durability barriers remain mandatory;
  slower filesystems use relative regression gates rather than these absolute
  local reference values.
- No test or implementation path requires QUIC negotiation to open or migrate
  local state. Focused primitive, filesystem fixture, corruption, and restart
  tests pass, followed by the full documented validation suite.
