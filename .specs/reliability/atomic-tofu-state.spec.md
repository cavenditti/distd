---
id: TASK:reliability/atomic-tofu-state
type: task
status: accepted
summary: >
  Persist first-use server trust and client enrollment identity atomically and
  fail closed when durable trust cannot be established.
owners: [carlo]
progress: pending
refines:
  - REQ:reliability/durable-state-contract#c-envelope
  - REQ:reliability/durable-state-contract#c-canonical
  - REQ:reliability/durable-state-contract#c-migration
  - REQ:reliability/durable-state-contract#c-tofu
aspects: [envelope, canonicalization, migration, tofu]
assignee: carlo
eta:
blocked_by: [TASK:reliability/durable-state-envelopes]
---

# Atomic tofu state

## Plan

Replace direct truncating JSON writes in [client persistence](spec:src:client/src/persistence.rs)
with the canonical durable envelope and a same-directory atomic write. The
state model MUST be keyed by canonical server URL, and each value MUST contain
that server's observed certificate pin and assigned UUID as one record. In
[client enrollment](spec:src:client/src/client.rs), sync and install the whole
candidate record before returning a usable client, or close/fail without
weakening the prior record. Connection and re-enrollment lookups MUST select pin
and UUID from the same URL key; a global fallback UUID is forbidden. Configured
explicit pins remain authoritative. Certificate rotation UX, publisher trust,
OS keychains, and multi-process client coordination are non-goals.

## Acceptance

- First-use success stores `{certificate pin, client UUID}` under the canonical
  server URL; restart obtains both fields from that record, rejects a different
  certificate without TOFU, and never sends server A's UUID to server B.
- Fault injection after create, encode, write, file sync, rename, and directory
  sync leaves either the complete previous envelope or complete new envelope;
  enrollment fails if the first pin cannot be durably committed.
- Truncated, corrupt, wrong-kind, and future-version state never silently
  becomes an empty trust store or permits a fresh TOFU decision; diagnostics do
  not expose enrollment tokens.
- Equivalent URL records inserted in different orders produce identical
  current-envelope bytes, while declared legacy JSON is migrated atomically and
  compared semantically.
- Multiple server URL records with different pins and UUIDs retain their exact
  field values when another entry changes, URL canonicalization cannot alias
  distinct authorities, explicit configured pins are not overwritten, focused
  cross-server/restart tests pass, and the full validation suite succeeds.
