---
id: REQ:reliability/bounded-publication-ingress
type: requirement
status: accepted
level: MUST
summary: >
  The REST publisher must reserve authenticated multipart payload bytes from a
  configured aggregate budget before it buffers fields or prepares storage.
owners: [carlo]
refines: [REQ:product/artifact-delivery#c-publication-ingress]
categorized_under: []
---

# Bounded publication ingress

## Context

The REST endpoint caps one multipart request at 512 MiB and then buffers every
accepted file field before chunking. Without aggregate admission, concurrent
authenticated uploads can therefore retain an unbounded multiple of that cap.
The reservation belongs after credential and publisher-policy prechecks, so
unauthorized traffic cannot consume publication capacity, but before multipart
extraction allocates or retains any field payload.

:::{requirement id="bounded-publication-ingress" level="MUST"}
- {#c-config} The server MUST expose one aggregate publication-body budget with
  a 512 MiB default and a strict 1..=2,048 MiB configuration range. Invalid,
  empty, noncanonical, or overflowing input MUST fail startup before state
  load, listeners, or storage mutation.
- {#c-declaration} `POST /items` MUST require one valid `Content-Length` and
  continue to reject actual request bodies above the fixed 512 MiB per-request
  limit. Chunked or otherwise undeclared publication bodies MUST fail before
  multipart extraction.
- {#c-admission} After bearer and publisher-header authorization, but before
  multipart extraction, the server MUST atomically reserve the declared body
  length from the aggregate budget. A request larger than the entire budget
  MUST fail as too large; temporary aggregate exhaustion MUST fail fast without
  queueing or consuming body bytes.
- {#c-lifetime} The byte-weighted reservation MUST remain owned until the
  request has finished validation, storage preparation, durable publication,
  and response construction, and MUST release on success, rejection,
  cancellation, panic unwind, or connection loss.
- {#c-progress} The server MUST apply one absolute configurable deadline from
  successful admission through complete multipart field extraction. A stalled
  or trickled body that misses the deadline MUST release all partial field
  buffers and its reservation without starting storage preparation or durable
  publication. The network deadline MUST stop after extraction so it cannot
  cancel an already-owned publication coordinator.
- {#c-observability} Rejections MUST use stable credential-free HTTP statuses
  and logs MUST NOT include body bytes, bearer tokens, publisher keys, or
  signatures. Test observability MUST prove exact reservation and release
  without influencing production admission.
:::

## Non-goals

This requirement does not stream multipart fields directly into chunk
preparation, claim a process-RSS bound, include parsing metadata or storage
allocations in the configured byte budget, or permit publication without a
declared HTTP body length. It does not weaken the existing publisher signature,
per-request body, path, or durable-commit checks.
