---
id: TASK:trust/signature-envelope
type: task
status: accepted
summary: >
  Define and test the canonical publisher statement and detached Ed25519
  signature envelope shared by server and client.
owners: [carlo]
progress: done
refines:
  - REQ:trust/publisher-authenticity#c-statement
  - REQ:trust/publisher-authenticity#c-key-policy
aspects: [statement, key-policy]
assignee: carlo
eta:
blocked_by: [TASK:security/loopback-admin-http]
---

# Signature envelope

## Plan

Add a core-domain signature envelope with an explicit format version and domain
separator, deterministic canonical encoding, Ed25519 verification, and a
BLAKE3 public-key fingerprint. The statement is derived from the existing
[manifest and chunk model](spec:src:core/src/item.rs). The dependent signed
publication task will add key and detached-signature fields to the
[sync schema](spec:src:core/proto/distd.proto); those fields will never define a
second statement serialization. Signing private-key storage, a signing CLI,
PKI, rotation, and revocation are not part of this task.

The publisher key authorizes only the canonical content statement. The server,
transport, persisted metadata, TLS identity, enrollment token, and revision
allocator are not signing authorities. Publisher trust policy is an explicit
set of exact 32-byte public keys in a later task; a displayed fingerprint never
grants trust by itself. Replay, rollback selection, and server-assigned revision
ordering remain explicit non-goals.

The V1 statement is a manually encoded, exact little-endian byte contract; it
does not use Serde, Bitcode, or Protobuf and callers cannot supply already
encoded bytes for verification. Its bytes are, in order:

1. `"distd publisher statement v1\0"`, followed by `u16(1)` and the exact
   remaining body length as `u32`;
2. a nonempty UTF-8 artifact ID as `u16 byte_length || bytes`, capped at 256
   bytes, followed by root hash `[u8; 32]` and total size `u64`;
3. one chunk-algorithm tag `u8` (`0` Fixed256K, `1` FixedAligned, `2` FastCdc,
   `3` BlockAlignedFastCdc, `4` ArchiveAwareFastCdc), followed by every exact
   variant field in declaration order as `u32`, except each seed as `u64`;
4. chunk count `u32` and the ordered chunks as repeated
   `hash:[u8; 32] || size:u64`;
5. entry count `u32` and each existing strictly ordered entry as
   `u16 path_length || UTF-8 path || size:u64 || start:u32 || end:u32`.

The signed fields are exactly artifact ID, root, total size, ordered chunk
hashes and sizes, the complete chunking profile, and ordered directory entries.
Server-assigned revision/version, deployment path, name, description, build
version, and timestamps are deliberately unsigned. Changing both redundant
revision fields consistently therefore leaves statement bytes unchanged;
rollback protection is a later policy concern. Redundant manifest chunk count
and nominal chunk size are not encoded, but construction rejects disagreement
with the authoritative chunk list or algorithm.

Construction and decode both require the existing manifest semantics:
metadata/manifest identity and revision agreement for `Item`, exact algorithm
nominal size, nonempty ordered chunk list, checked root and total
reconstruction, and canonical sorted unique directory paths with contiguous
ranges. Additionally, artifact IDs and entry paths must be valid UTF-8 NFC.
Paths use `/` separators on every platform and reject empty components, `.`,
`..`, leading or trailing `/`, backslash, NUL, reserved first components, and
ancestor-file conflicts; sorting is by the stored UTF-8 bytes and decode never
normalizes. Known non-fixed profiles require profile version 1 and exact valid
nonzero size/alignment relationships; unknown versions and tags are rejected
rather than defaulted or clamped. Each chunk size must fit `u32` for lossless
transport. A zero-size chunk must have exactly the `BLAKE3(empty)` hash. A
single empty artifact is one such chunk; within a directory, each empty entry
covers exactly one such chunk and nonempty entries cover only nonempty chunks.

Statement limits are 65,536 chunks, 65,536 entries, 4,096 UTF-8 bytes per
entry path, and the existing 32 MiB frame-payload bound for total canonical
bytes. Counts and lengths are checked against both limits and remaining input
before allocation. Peak construction/decoding allocation attributable to this
module is bounded by the canonical byte limit plus 8 MiB, covering the exact
maximum chunk/entry record vectors without retaining a second canonical copy.
Decode consumes exact EOF, fully validates the typed content, and accepts only
the one structurally canonical fixed-width encoding.

The detached V1 envelope is fixed-width:
`"distd publisher signature envelope v1\0" || envelope_version:u16(1) ||
statement_version:u16(1) || public_key:[u8;32] || signature:[u8;64]`, with no
trailing bytes. Ed25519 verification uses the existing Ring dependency and
signs only the complete domain-separated statement without a prehash. The
publisher fingerprint is
`BLAKE3("distd publisher public key fingerprint v1\0" || raw_public_key)`;
its text form is exactly 64 lowercase hexadecimal characters and is distinct
from the TLS certificate fingerprint.

Add a dedicated non-exhaustive core error type and fixed-size public key,
signature, fingerprint, statement, and envelope types with byte accessors and
checked slice conversions. Verification reconstructs the statement internally
from a fully validated `Item`; it never trusts Protobuf, Bitcode, or a caller's
statement bytes as content authority. Core also exposes exact statement and
envelope decode for fixtures/tooling, but no private-key loading, signing,
trust allowlist, or policy API. A policy-layer unknown publisher is not a
cryptographic-format error. Later transport conversion must be strict and
fallible: the existing lossy/defaulting `ChunkAlgorithm::from_proto` cannot be
used at this trust boundary.

## Acceptance

- Repeated construction and encode/decode produce identical canonical bytes,
  and the format is domain-separated and explicitly versioned.
- Changing any covered artifact ID, root, size, chunk order/hash/size, chunking
  parameter, entry path, entry size, or entry range invalidates verification.
  Consistently changing only unsigned revision, deployment, descriptive, or
  timestamp metadata does not change statement bytes.
- Malformed keys/signatures, unknown format versions, trailing or ambiguous
  encodings, and signatures from a different key are rejected without panic.
- Independent nonempty statement and envelope literals are hash-pinned and
  round-trip byte-identically. Mutation tests cover every field and algorithm
  variant; malformed tests cover domains, versions, truncation, trailing data,
  invalid UTF-8, noncanonical paths/ranges, count and size mismatches, exact
  bounds and one above, and one-byte signed-statement mutations.
- Deterministic generated valid manifests round-trip through the independent
  decoder and verify under an independently generated Ring Ed25519 key, while
  another key, malformed fixed-size inputs, and any signed-field mutation fail.
- An ignored release reference reports canonical construction and signature
  verification for representative single-chunk, 64-chunk, 4,096-chunk, and
  maximum-count manifests. Reference p95 ceilings are: fingerprint 10 us;
  construction 100 us at 1/64 chunks, 1 ms at 4,096, and 10 ms at 65,536;
  verification 100 us at 1/64, 1 ms at 4,096, and 15 ms at 65,536. Raw samples,
  exact source/lock identity, environment, warmups, and sample counts are
  disclosed; these are local regression gates, not portable guarantees.
- Focused core unit/property tests pass, followed by the repository validation
  commands documented in the [README](spec:src:README.md).
