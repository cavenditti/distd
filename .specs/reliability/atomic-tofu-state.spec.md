---
id: TASK:reliability/atomic-tofu-state
type: task
status: accepted
summary: >
  Persist first-use server trust and client enrollment identity atomically and
  fail closed when durable trust cannot be established.
owners: [carlo]
progress: done
refines:
  - REQ:reliability/durable-state-contract#c-envelope
  - REQ:reliability/durable-state-contract#c-canonical
  - REQ:reliability/durable-state-contract#c-migration
  - REQ:reliability/durable-state-contract#c-tofu
aspects: [envelope, canonicalization, migration, tofu]
assignee: carlo
eta:
blocked_by: []
---

# Atomic tofu state

## Plan

Replace direct truncating JSON writes in [client persistence](spec:src:client/src/persistence.rs)
with durable-state kind `ClientTrust = 2`, payload schema `1`, a 1 MiB payload
ceiling, at most 4,096 records, and a same-directory atomic write to
`distd_client_trust.state`. The current payload is a flat manual binary codec:
`u32` little-endian record count followed by records in strictly increasing
canonical-URL byte order; each record is `u16` little-endian URL length, UTF-8
URL bytes (at most 512 bytes), a 32-byte certificate pin, and a 16-byte non-nil
UUID. Decoding MUST validate counts, lengths, UTF-8, URL canonicality, strict
ordering, duplicate absence, exact exhaustion, and all bounds before allocating
the record map. Newly encoded envelope and payload bytes MUST have golden
fixtures.

Add a canonical server URL type shared by persistence and transport. It MUST
accept only `quic://` and the existing `udp://` alias with an explicit non-zero
`u16` port and no userinfo, non-root path, query, fragment, percent-encoded
authority, IPv6 zone, or non-ASCII DNS name. Both schemes canonicalize to
`quic://`; DNS names are lowercased and lose one terminal dot, IP literals and
decimal ports use their canonical text, IPv6 remains bracketed, and an empty
path equals `/`. Canonicalization MUST NOT resolve DNS. Different normalized
hosts or ports remain different authorities.

The state model MUST be keyed by this canonical URL, and each value MUST
contain that server's observed certificate pin and assigned UUID as one record.
The old `distd_client_state.json` format is the only supported legacy format:
`{client_uuid: Option<String>, server_certificates: object}`. Its input is
bounded to 1 MiB and duplicate JSON keys, unknown fields, invalid pins/UUIDs,
or incomplete pairs are rejected. Empty state migrates to empty state. A global
UUID may migrate only when all raw certificate keys reduce to exactly one
canonical authority with one identical pin; multiple distinct authorities are
ambiguous and fail closed. Migration writes and rereads the new envelope,
compares semantics, and retains the exact JSON source. Presence of a current
file always suppresses legacy fallback, including when current bytes are
corrupt, wrong-kind, or future-version.

In [client enrollment](spec:src:client/src/client.rs), introduce a pending
registered server that cannot fetch metadata or escape as a usable `Server`.
Select pin and UUID together from one canonical record, register, durably
install the whole candidate record using blocking-I/O isolation, and only then
activate/fetch and return a `Client`. Commit failures, including indeterminate
post-rename directory-sync failures, drop the pending connection, leave the
prior in-memory record unchanged, and fail construction. An existing identical
record is a no-op; any different pin or UUID is a conflict. A configured
explicit pin is authoritative: it may create a missing record, but a conflict
with an existing record fails before connection rather than performing implicit
certificate rotation. Enrollment-token values MUST be redacted from debug and
error diagnostics. Certificate rotation UX, publisher trust, OS keychains, and
multi-process client coordination are non-goals.

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
- URL equivalence and rejection matrices, exact current and legacy golden
  fixtures, ambiguous legacy ownership, canonical collisions, record and size
  boundaries, configured-pin conflict, activation ordering, and every durable
  replacement fault class are covered. Shared atomic-stage tests plus the
  client commit-failure seam prove enrollment never returns a usable server on
  failure and prior memory remains unchanged.
- On the reference local APFS host, canonical encode plus integrity and
  load/verify/decode each remain at or below 1 ms for 1,000 records, while a
  complete file-sync/rename/directory-sync commit remains at or below 15 ms
  p95. Filesystem I/O runs outside Tokio transport locks and outside the async
  executor worker.
