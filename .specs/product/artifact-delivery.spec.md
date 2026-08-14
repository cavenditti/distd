---
id: REQ:product/artifact-delivery
type: requirement
status: accepted
level: MUST
summary: >
  distd delivers immutable artifact revisions efficiently without allowing
  unverified or remotely redirected content to become active.
owners: [carlo]
refines: []
categorized_under: []
---

# Artifact delivery

## Context

v0.2 establishes authenticated single-origin QUIC synchronization, stable
artifact IDs, immutable versions, delta transfer, verified staging, and atomic
replacement. These properties form the product contract that later trust and
distribution refinements must preserve. See the [manifest model](spec:src:core/src/item.rs),
[sync protocol](spec:src:core/proto/distd.proto), and [activation path](spec:src:core/src/chunk_storage/fs_storage.rs).

:::{requirement id="artifact-delivery" level="MUST"}
- {#c-identity} A published artifact MUST have a stable operator-selected
  identity, and every distinct accepted revision MUST remain addressable as an
  immutable version of that identity.
- {#c-integrity} A client MUST validate every received or reused chunk and the
  complete ordered manifest root before materializing an artifact.
- {#c-authenticity} A managed client MUST be able to authenticate the software
  publisher independently of the distribution server and its TLS certificate.
- {#c-admin-exposure} Plaintext publication and administration endpoints MUST
  bind only to loopback by default and MUST require an explicit insecure
  override before binding to a non-loopback address.
- {#c-destination} Server-controlled metadata MUST NOT select an absolute,
  parent-traversing, or symbolic-link-mediated client destination.
- {#c-activation} A failed authentication, validation, transfer, staging, or
  replacement MUST leave the previously active destination intact.
- {#c-efficiency} Synchronization SHOULD transfer only missing content and MUST
  keep individual protocol frames within configured bounds.
:::

## Non-goals

This requirement does not require P2P distribution, a swarm protocol, release
eligibility rules, downgrade/rollback decisions, key transparency, or bounded
process RSS. Missing-payload retention is bounded by the
[client sync memory requirement](spec:req:REQ:reliability/bounded-client-sync-memory);
transport metadata, one currently decoded frame, operating-system page cache,
and unrelated process allocations remain outside that configured boundary.
