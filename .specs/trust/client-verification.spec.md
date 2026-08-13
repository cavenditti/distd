---
id: TASK:trust/client-verification
type: task
status: accepted
summary: >
  Fail closed on untrusted sync manifests before chunk transfer or destination
  activation while preserving the previously active artifact.
owners: [carlo]
progress: pending
refines:
  - REQ:trust/publisher-authenticity#c-client
  - REQ:product/artifact-delivery#c-activation
aspects: [authenticity, activation]
assignee: carlo
eta:
blocked_by: [TASK:trust/signed-publication]
---

# Client verification

## Plan

Add an explicit client publisher-key allowlist distinct from the existing TLS
fingerprint in [client settings](spec:src:client/src/settings.rs). In the
[manifest-first client flow](spec:src:client/src/server/mod.rs), reconstruct and
verify the canonical statement immediately after the manifest response and
before sending a possession bitfield. Surface a distinct authenticity error;
only a verified manifest may reach the existing [staging activation path](spec:src:core/src/chunk_storage/fs_storage.rs).
Rollback/downgrade selection, trust-on-first-use for publisher keys, and legacy
unsigned acceptance in secure mode are non-goals.

## Acceptance

- A valid independently signed single-file and directory artifact completes a
  cold fetch and delta update under an allowed publisher key.
- Unsigned, unknown-key, malformed, and tampered manifest fields fail before
  the possession message is sent; no chunks are accepted, the prior
  destination remains byte-identical, and local active metadata is unchanged.
- Reusing the pinned TLS server key as a publisher key fails unless that exact
  public key is separately present in the publisher allowlist.
- Client configuration tests, QUIC integration tests, staging regressions, and
  the full documented validation suite pass.
