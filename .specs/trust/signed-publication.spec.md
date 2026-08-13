---
id: TASK:trust/signed-publication
type: task
status: accepted
summary: >
  Require, verify, persist, and serve publisher signatures at the publication
  boundary before a revision becomes visible.
owners: [carlo]
progress: pending
refines:
  - REQ:trust/publisher-authenticity#c-publication
  - REQ:trust/publisher-authenticity#c-propagation
aspects: [publication, propagation]
assignee: carlo
eta:
blocked_by: [TASK:trust/signature-envelope]
---

# Signed publication

## Plan

Configure an explicit server allowlist of publisher public keys. Extend the
authenticated [HTTP publication boundary](spec:src:server/src/rest_api.rs) to
accept a key identity and detached signature, derive the canonical statement
from the server-computed artifact, and reject invalid publication before the
[server metadata transaction](spec:src:server/src/server.rs) advances. Persist
the accepted envelope with every immutable revision and include it in the QUIC
manifest response. Private signing keys and orphan-chunk garbage collection are
non-goals; failed validation may leave unreachable chunks but never visible
metadata.

## Acceptance

- Missing, malformed, unknown-key, mismatched-artifact, and invalid signatures
  return a non-success publication response and leave latest/version metadata
  byte-for-byte unchanged across restart.
- A valid signature produces exactly one immutable visible revision; an
  idempotent repeat does not create a new version, and stored key/signature
  bytes round-trip unchanged through persistence and manifest sync.
- Concurrency tests prove signature validation and version advancement occur
  under the existing publication serialization boundary.
- Existing bearer-token and payload-size controls remain effective, focused
  server integration tests pass, and the full documented validation suite
  passes.
