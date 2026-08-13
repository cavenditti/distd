---
id: REQ:reliability/durable-state-contract
type: requirement
status: accepted
level: MUST
summary: >
  Durable server, storage, activation, and client state is independently
  versioned, canonical, recoverable, and bounded.
owners: [carlo]
refines: []
categorized_under: []
---

# Durable state contract

## Context

[Server metadata](spec:src:server/src/server.rs) and
[filesystem-storage state](spec:src:core/src/chunk_storage/fs_storage.rs) use
raw codec output, while [client trust persistence](spec:src:client/src/persistence.rs)
uses unversioned JSON and direct truncation. Current state includes unordered
maps and sets, so raw serializer output cannot be promised byte-stable until
entries are canonicalized. Durable compatibility is independent of the public
QUIC wire contract and may be implemented first.

:::{requirement id="durable-state-contract" level="MUST"}
- {#c-envelope} Every durable server metadata, filesystem-storage, activation,
  and client trust document MUST carry a kind-specific versioned envelope with
  magic, kind, schema version, checked payload length, integrity validation, and
  rejection of trailing data before payload decoding.
- {#c-canonical} A newly written current-version document MUST use a canonical
  representation: unordered map and set entries MUST be sorted by their encoded
  key/value identity before serialization. Current-version golden fixtures MUST
  therefore be byte-stable; legacy fixtures require semantic equivalence unless
  their historical encoding was already canonical.
- {#c-migration} Each declared-supported state upgrade MUST be deterministic,
  atomic, and fixture-tested. Corrupt, wrong-kind, truncated, oversized, or
  unsupported state MUST preserve its source bytes and fail or enter an explicit
  recovery mode rather than silently starting fresh.
- {#c-tofu} Trust-on-first-use state MUST be an atomic mapping from canonical
  server URL to a record containing that server's certificate pin and assigned
  client UUID. A UUID learned from one server MUST never be sent to another,
  and first enrollment MUST not become usable until the complete record is
  durable.
- {#c-cache} The configured client hot-chunk cache ceiling MUST be a hard bound
  on cache-owned payload bytes, including duplicate accesses and external
  references; zero MUST disable retention.
:::

## Non-goals

This requirement does not change QUIC negotiation, promise support for every
historic build, coordinate concurrent writers from multiple processes, or
bound transfer staging, caller-owned buffers, allocator RSS, or the OS page
cache.
:::
