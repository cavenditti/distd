---
id: TASK:trust/release-sequence-envelope
type: task
status: accepted
summary: >
  Define the canonical V2 publisher release statement that signs an
  artifact-scoped monotonic sequence without weakening the frozen V1 format.
owners: [carlo]
progress: done
refines:
  - REQ:trust/rollback-protection#c-sequence
  - REQ:trust/rollback-protection#c-compatibility
aspects: [sequence, compatibility]
assignee: carlo
eta:
blocked_by: []
---

# Release sequence envelope

## Plan

Extend the core [publisher signature module](spec:src:core/src/signature.rs)
with a type-distinct V2 release statement and envelope. V1 bytes and APIs remain
frozen and byte-identical; V2 is not accepted through a V1 verification method.
This task defines cryptographic bytes only. Server publication headers,
metadata migration, QUIC propagation, client configuration, and durable
high-water enforcement are dependent tasks and MUST NOT invent a second
serialization.

The V2 statement is manually encoded in little endian as:

1. exact domain `"distd publisher release statement v2\0"`, followed by
   `format_version:u16=2` and `body_length:u32`;
2. `artifact_length:u16 || artifact_utf8`, then
   `publisher_sequence:u64`;
3. the V1 authoritative content body in the same order and encoding: root hash,
   total size, full chunk-algorithm tag and parameters, ordered chunk count and
   `{hash,size}` records, then ordered entry count and
   `{path,size,start,end}` records.

The sequence accepts the complete `u64` range, including initial sequence zero.
It is publisher-controlled and independent of both redundant server revision
fields. V2 construction applies every existing V1 content, path, profile,
transport-width, root, count, and 32 MiB canonical-size validation. The extra
sequence field counts toward that same cap. Decode checks declared body length,
preflights counts and lengths before allocation, consumes exact EOF, and
re-encodes byte-identically.

The V2 detached envelope is fixed width:
`"distd publisher release envelope v2\0" || envelope_version:u16=2 ||
statement_version:u16=2 || public_key:[u8;32] || signature:[u8;64]`.
Ed25519 signs the complete V2 statement directly. It reuses the exact raw-key
type and publisher fingerprint algorithm, but has a distinct Rust type and
decode path from `PublisherSignatureEnvelope`. Define a fixed 32-byte release
statement identity as
`BLAKE3("distd publisher release identity v1\0" || exact_v2_statement_bytes)`;
later durable policy stores this identity with the sequence.

Public construction and verification accept typed `Item` or `Manifest` plus a
`PublisherReleaseSequence`; callers cannot supply content bytes independently
of the typed revision. Expose exact byte accessors, strict decode, sequence and
identity accessors, and manifest-first verification. No private-key API,
allowlist, transport field, server revision policy, or high-water storage is
added here.

## Acceptance

- Independent nonempty directory V2 statement and envelope literals are
  hash-pinned, verify with an independently constructed Ring Ed25519 signature,
  and decode/re-encode byte-identically while all V1 goldens remain unchanged.
- Changing the sequence or any V1-covered content field invalidates the
  original signature; consistently changing only server revision or descriptive
  metadata leaves V2 bytes unchanged.
- V1 and V2 domains, versions, envelope types, and verification methods reject
  cross-decoding, unknown versions, truncation, trailing bytes, malformed
  keys/signatures, and wrong-key signatures without panic.
- Exact sequence `0` and `u64::MAX`, 32 MiB statement, count/path bounds, every
  canonical chunk algorithm, empty artifact, and generated file/directory
  round trips are covered. V2 size is exactly the corresponding V1 canonical
  content size plus the declared domain-length delta and eight sequence bytes.
- A deterministic generated-property matrix proves semantic inequality of
  sequence or covered content yields different statement identity and that
  arbitrary one-byte mutations never verify under the original envelope.
- The ignored release reference includes V2 file and directory construction and
  verification at 1, 64, 4,096, and 65,536 chunks. It retains the existing V1
  p95 gates (100 us at 1/64, 1 ms at 4,096, construction 10 ms and verification
  15 ms at 65,536), reports raw samples and exact source identity, and confirms
  one exact-capacity canonical allocation with no second encoding.
- Focused core tests, strict correctness/suspicious Clippy, stable and nightly
  workspace suites, formatting, diff checks, and `spec lint` pass.

## Verification evidence

The final release reference ran with 5 warmups and 20 measured samples per V1
and V2 file/directory workload on arm64 macOS. The source snapshot BLAKE3 was
`6c66f86de6119e586de53a0fd95f42038947b0ba303541710caa6da9522581e1`.
V2 construction/verification p95s were 0.125/20.458 us at one file chunk,
3.458/32.167 us at 64 file chunks, 0.222/0.503 ms at 4,096 file chunks, and
3.664/7.806 ms at 65,536 file chunks. Directory p95s were 0.208/32.833 us,
4.542/33.583 us, 0.279/0.702 ms, and 4.516/10.865 ms respectively. All frozen
gates passed, and the same run reconfirmed every V1 gate and golden.

The focused signature suite passed 21 tests with the release reference ignored;
strict core correctness/suspicious Clippy passed; stable workspace and nightly
all-features workspace suites passed; and formatting, diff checks, and
`spec lint` passed. The first sandboxed stable run failed only because a bench
test could not bind a loopback UDP socket; the identical approved loopback run
passed.
