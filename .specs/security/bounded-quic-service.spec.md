---
id: REQ:security/bounded-quic-service
type: requirement
status: accepted
level: MUST
summary: >
  QUIC admission and deadlines bound attacker-controlled work before and after
  authentication so one peer cannot exhaust server memory or transfer slots.
owners: [carlo]
refines: []
categorized_under: []
---

# Bounded quic service

## Context

The server currently advertises 32 bidirectional streams per connection and a
32 MiB stream receive window, while the generic framed reader allocates the
declared payload before decoding or validating the enrollment token. An
unauthenticated connection can therefore reserve roughly 1 GiB across 32
registration streams, and stream handling has no request or transfer deadline.
See the [QUIC transport configuration and stream loop](spec:src:server/src/quic.rs)
and [length-delimited reader](spec:src:core/src/utils/frame.rs).

:::{requirement id="bounded-quic-service" level="MUST"}
- {#c-preauth} Before authentication succeeds, a peer MUST NOT cause allocation
  proportional to the general 32 MiB frame ceiling. A Register frame MUST be
  rejected from its length prefix before allocation when it exceeds 64 KiB.
- {#c-admission} Admission MUST be fail-fast and acquired before spawning
  stream work, allocating a framed payload, or accessing storage. The initial
  hard ceilings MUST be 64 pending streams globally and four per connection,
  with every pending stream reserving 64 KiB from a 4 MiB global application
  budget; active authenticated transfers MUST be limited to four globally and
  two per connection, with every transfer reserving a conservative 64 MiB from
  a 256 MiB global application budget. After fixed-prelude UUID authentication,
  an active count and byte guard MUST be owned before reading even the next
  request length prefix, not merely before allocating its declared payload.
- {#c-accounting} For configured values `P`, `T`, `Bpre`, and `Bactive`, startup
  MUST check `P * 64 KiB <= Bpre` and `T * 64 MiB <= Bactive` without overflow,
  and runtime MUST maintain `pending * 64 KiB <= Bpre` and
  `active * 64 MiB <= Bactive`. Current and peak counters MUST describe these
  application reservations, not allocator RSS, transport buffers, or kernel
  memory.
- {#c-deadlines} TLS/QUIC handshake, the per-connection `accept_bi` idle wait,
  stream prelude, optional UUID, registration/request, possession, and response
  write progress MUST each have a finite configurable deadline. Each stream
  MUST also have an absolute lifetime that progress cannot renew, so trickled
  bytes cannot retain capacity indefinitely. Admission MUST use an immediate
  attempt rather than an unbounded permit wait; any later waiting policy MUST
  have its own finite deadline.
- {#c-release} Every success, parse/auth failure, rejection, timeout,
  cancellation, and connection close MUST release permits and reservation
  counters through owned RAII guards. Rejected or timed-out streams MUST
  explicitly stop their receive side and reset their send side.
- {#c-config} Defaults MUST equal the initial ceilings, configurable values MUST
  be positive and no greater than them, per-connection counts MUST not exceed
  global counts, and every duration MUST be positive and at most 24 hours with
  the absolute lifetime no shorter than any phase deadline. Empty, non-UTF-8,
  overflowed, or inconsistent settings MUST fail startup before secrets or
  storage are loaded, using diagnostics that contain no credentials or
  payloads. Rejection and timeout events MUST identify only phase and reason.
:::

## Non-goals

This requirement does not add IP reputation, distributed rate limiting, proof
of work, or DDoS protection upstream of the host. It does not replace the
existing frame-size and decompression limits; those remain defense in depth.
The reservation counters do not promise an exact process-RSS ceiling or
immediate return of freed pages to the operating system. Deadlines are enforced
at asynchronous operations and between bounded batch-building steps; hard
preemption of synchronous filesystem reads or compression requires a separate
cancel-aware storage/worker design.
:::
