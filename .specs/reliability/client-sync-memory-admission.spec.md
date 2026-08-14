---
id: TASK:reliability/client-sync-memory-admission
type: task
status: accepted
summary: >
  Enforce a checked client missing-payload budget at the verified-manifest
  capability boundary before possession and durable rollback acceptance.
owners: [carlo]
progress: done
refines:
  - REQ:reliability/bounded-client-sync-memory#c-config
  - REQ:reliability/bounded-client-sync-memory#c-accounting
  - REQ:reliability/bounded-client-sync-memory#c-boundary
  - REQ:reliability/bounded-client-sync-memory#c-retention
  - REQ:reliability/bounded-client-sync-memory#c-observability
assignee: carlo
eta:
blocked_by: []
aspects: [configuration, admission, accounting, retention, evidence]
---

# Client sync memory admission

## Plan

Add `client.max_sync_memory_mib` as a validated integer with a 512 MiB default
and 1..=1,024 MiB accepted range. Convert it once to bytes with checked
arithmetic during settings construction, carry the immutable byte budget across
pending enrollment, activation, cloning, and reconnect, and redact no less than
the existing client/server Debug surfaces.

Extend the private verified-manifest result with the exact missing-payload byte
sum calculated during the existing possession pass. Each chunk whose hash is
present in the local basis contributes zero; every absent ordered occurrence
contributes its declared `u32` transport size. Reject checked overflow or a sum
above the budget inside the same blocking verification worker. Perform this
check after strict typed conversion and publisher verification, but before the
durable release watermark, `VerifiedSync` construction, possession, chunk
decode, cache/storage calls, or activation. Keep exact-at-limit admission and
the existing shared-backing receive representation.

Expose a distinct `ServerRequest` error containing only required and configured
byte counts. Add pure boundary/accounting tests, capability tests with an
observed continuation, durable-watermark byte-identity checks, configuration
default/boundary failures, repeated-hash and local-reuse cases, and a real
signed QUIC rejection that observes no possession or destination mutation.
Retain the existing cold/delta/directory product integration and cache ceiling
tests. Measure the fused 65,536-chunk verification path in the existing ignored
release reference and require no regression beyond its frozen 20 ms p95 / 35 ms
max worker gates; the accounting pass must not allocate a second hash set or
walk the manifest again.

## Acceptance

- Missing payload below and exactly at 512 MiB is admitted by the default;
  512 MiB plus one byte is rejected before possession. Configured 1 and 1,024
  MiB boundaries parse, while zero, 1,025 MiB, negative, fractional, string,
  overflow, and other non-integer configuration input fail before client
  enrollment.
- The accounting result is identical to the emitted possession bitfield for
  no local chunks, complete local possession, partial reuse, repeated hashes,
  zero-size empty artifacts, and the maximum chunk count. Every addition is
  checked; exact-at-limit is accepted and limit-plus-one is rejected.
- Every over-budget signed response resets its continuation and reports zero
  possession sends, received chunks, activation records, and destination
  changes. Its rollback-state file is absent or byte-identical, and two
  restarts preserve the previously active revision.
- Valid signed file and directory cold/delta syncs below the budget still
  activate exact bytes. Bulk receive allocations remain shared and the hot
  cache's independent ceiling continues to hold.
- The ignored release verification reference still passes all existing
  1/64/4,096/65,536 worker, coordination, heartbeat, policy, and lock gates on
  the exact final source. Focused tests, strict configured Clippy, formatting,
  spec lint, and the full locked/offline workspace suite pass.
