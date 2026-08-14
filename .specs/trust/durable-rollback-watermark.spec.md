---
id: TASK:trust/durable-rollback-watermark
type: task
status: accepted
summary: >
  Propagate signed release sequences and enforce an atomic, durable client
  high-water mark before verified activation.
owners: [carlo]
progress: pending
refines:
  - REQ:trust/rollback-protection#c-decision
  - REQ:trust/rollback-protection#c-durability
  - REQ:trust/rollback-protection#c-boundary
aspects: [decision, durability, boundary]
assignee: carlo
eta:
blocked_by: [TASK:trust/release-sequence-envelope]
---

# Durable rollback watermark

## Plan

After the V2 release statement is frozen, add strict REST publication input,
one immutable server metadata attestation, exact QUIC propagation, client
verification, and a canonical-server-plus-artifact durable high-water record.
The record stores the maximum publisher sequence and exact V2 statement
identity. Lower sequences and equal-sequence identity conflicts fail before
possession; an exact equal release is idempotent; a higher valid release is
recorded by an owned cancellation-safe coordinator before activation may
replace the destination. Preserve V1 only behind an explicit visibly replayable
compatibility setting and never synthesize a sequence during migration.

Freeze a new durable-state kind/schema with canonical sorting, exact goldens,
bounded record counts and payload size, fail-closed current-state parsing,
deterministic migration, file and directory sync, atomic replace, reread
equivalence, all replacement-stage faults, and restart-twice convergence. The
server metadata schema and wire fields bind the typed V2 envelope and sequence
in the same immutable revision rather than parallel maps. The client must not
derive publisher authority from TLS pins, server revisions, timestamps, or
claimed fingerprints.

## Acceptance

- REST, metadata, QUIC, client, activation, fault/restart, cancellation, and
  concurrency matrices prove lower and conflicting releases never reach
  possession or mutate storage, while exact retries and monotonic releases
  converge durably.
- A fresh client limitation, explicit V1 replayable mode, key rotation within
  one server authority, removal/re-addition, maximum sequence, and sequence
  exhaustion are covered without hidden downgrade or reset.
- Frozen schema and wire goldens plus stable/nightly workspace validation,
  release performance references, strict Clippy, formatting, and spec lint pass.
