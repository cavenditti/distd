---
id: REQ:reliability/verified-client-materialization
type: requirement
status: accepted
level: MUST
summary: >
  Client activation must validate received and reusable bytes while treating
  only the caller-selected contained path as destination authority.
owners: [carlo]
refines:
  - REQ:product/artifact-delivery#c-integrity
  - REQ:product/artifact-delivery#c-destination
categorized_under: []
aspects: [integrity, destination-authority]
---

# Verified client materialization

## Context

Publisher verification authenticates the ordered manifest before possession,
but activation must still prove that the bytes supplied by the transfer or
reused from local storage implement that manifest. The remote catalog also
contains a deployment hint; it is useful for display but is not authority to
select a path on the client.

:::{requirement id="verified-client-materialization" level="MUST"}
- {#c-received} Every received chunk MUST have its declared size and hash
  verified before it can be admitted to activation staging or the hot cache.
- {#c-reused} Every ordinary reusable backing copied into a candidate MUST be
  read with a bounded buffer and hashed over the exact declared range before
  activation. An unchanged extent from the exact previously validated
  immutable active revision MAY satisfy this proof through its authenticated
  active-reference identity and filesystem seal; a seal mismatch on recovery
  MUST fall back to complete verification and fail closed on disagreement.
- {#c-root} Activation MUST reconstruct and compare the complete ordered root,
  total size, chunk coverage, and directory entry structure before selecting
  the candidate.
- {#c-destination} The activation destination MUST come only from the local
  caller or managed-client configuration, resolve beneath the storage root,
  and reject parent traversal and symbolic-link mediation. A remote catalog
  deployment hint MUST NOT cross the storage-worker authority boundary as a
  destination value; signed relative manifest entry paths remain content
  structure and MUST receive their existing canonical containment checks.
- {#c-failure} Any received-byte, reused-byte, root, structure, containment, or
  replacement failure MUST leave the previous active selector and destination
  intact; an initial failed activation MUST leave no active destination.
:::

## Non-goals

This requirement does not defend against a hostile local process racing
filesystem operations, require rehashing an unchanged sealed immutable extent
on every activation, authorize server-selected deployment paths, or weaken the
existing crash-consistent activation protocol.
