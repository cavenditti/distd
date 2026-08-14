---
id: REQ:reliability/bounded-client-sync-memory
type: requirement
status: accepted
level: MUST
summary: >
  A client must reject a synchronization whose missing payload exceeds its
  configured retention budget before it sends possession or advances durable
  rollback state.
owners: [carlo]
refines: [REQ:product/artifact-delivery#c-efficiency]
categorized_under: []
---

# Bounded client sync memory

## Context

The QUIC protocol bounds every individual frame, but the client currently
retains all missing chunks until verified atomic activation begins. A validly
signed manifest can therefore describe substantially more missing payload than
the client is prepared to retain. The bound belongs at the verified-manifest
capability boundary: after strict statement validation establishes the exact
ordered chunk list, but before possession authorizes the server to send any
chunk and before rollback state records the release as accepted.

:::{requirement id="bounded-client-sync-memory" level="MUST"}
- {#c-config} The client MUST expose one validated missing-payload retention
  budget. The default MUST be 512 MiB; zero and values above 1,024 MiB MUST fail
  configuration before enrollment, trust-state mutation, or network use.
- {#c-accounting} Admission MUST compute the checked sum of the declared sizes
  of exactly those ordered manifest chunks absent from the client's local
  possession basis. Repeated hashes MUST follow the same possession decision
  used on the wire, and arithmetic overflow MUST reject the manifest.
- {#c-boundary} A sum above the configured budget MUST reset the pending sync
  stream before sending possession, accepting chunks, scheduling storage, or
  advancing the durable publisher-release watermark. A sum exactly equal to
  the budget MUST remain admissible.
- {#c-retention} After admission, chunk ordering, size, decompression, hash, and
  completeness validation MUST preserve the invariant that retained decoded
  missing payload is no greater than the admitted sum. Bulk chunk views MAY
  share one backing allocation but MUST NOT duplicate its payload bytes merely
  to satisfy the storage API.
- {#c-observability} Rejection MUST use a distinct credential-free error that
  reports required and configured byte counts. Tests and benchmarks MUST prove
  the pre-possession boundary and exact accounting without logging publisher
  signatures, raw keys, enrollment tokens, or chunk contents.
:::

## Non-goals

This requirement does not stream chunks directly into activation staging,
bound publisher-side multipart buffering, define process RSS, include manifest
metadata or one currently decoded compressed frame in the configured payload
budget, or make the configured ceiling a portable performance guarantee. It
does not weaken authenticity, rollback, integrity, or atomic-activation checks.
