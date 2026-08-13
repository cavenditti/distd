---
id: TASK:trust/signature-envelope
type: task
status: accepted
summary: >
  Define and test the canonical publisher statement and detached Ed25519
  signature envelope shared by server and client.
owners: [carlo]
progress: pending
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
[manifest and chunk model](spec:src:core/src/item.rs); transport fields in the
[sync schema](spec:src:core/proto/distd.proto) carry the key and detached
signature but do not define a second serialization. Signing private-key
storage, a signing CLI, PKI, rotation, and revocation are not part of this task.

## Acceptance

- Repeated construction and encode/decode produce identical canonical bytes,
  and the format is domain-separated and explicitly versioned.
- Changing any covered artifact ID, root, size, chunk order/hash/size, chunking
  parameter, entry path, entry size, or entry range invalidates verification.
- Malformed keys/signatures, unknown format versions, trailing or ambiguous
  encodings, and signatures from a different key are rejected without panic.
- Focused core unit/property tests pass, followed by the repository validation
  commands documented in the [README](spec:src:README.md).
