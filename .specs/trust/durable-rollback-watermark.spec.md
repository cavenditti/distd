---
id: TASK:trust/durable-rollback-watermark
type: task
status: accepted
summary: >
  Propagate signed release sequences and enforce an atomic, durable client
  high-water mark before verified activation.
owners: [carlo]
progress: done
refines:
  - REQ:trust/rollback-protection#c-decision
  - REQ:trust/rollback-protection#c-durability
  - REQ:trust/rollback-protection#c-boundary
aspects: [decision, durability, boundary]
assignee: carlo
eta:
blocked_by: []
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

The server-side immutable attestation becomes one of three explicit variants:
`ReleaseV2 { sequence, envelope }`, `ReplayableV1(envelope)`, or
`UnsignedLegacy`. Required publisher policy accepts and emits only `ReleaseV2`;
the latter two variants remain readable and servable only in the existing
explicit insecure mode. Exact-content idempotence includes the variant,
sequence, public key, and signature bytes. A reused sequence with different
content is rejected by the client even when both signatures are valid.

REST V2 uses the existing exact singleton version/key/signature headers plus
`X-Distd-Publisher-Release-Sequence`, whose value is canonical unsigned decimal
with no sign, whitespace, or leading zero except `0`. Version 2 requires all
four headers; version 1 requires the three legacy headers and no sequence and
is admitted only by explicitly insecure server policy; absence of all four is
unsigned and likewise insecure-only. The body limit and bearer-first ordering
remain unchanged.

Server publication payload schema 3 uses magic `DSPUBV3\0`. Attestation tag 0
is unsigned legacy, tag 1 is replayable V1 followed by the exact 32-byte key and
64-byte signature, and tag 2 is V2 followed by `sequence:u64`, exact key, and
exact signature. The previous schema-2 file remains at
`metadata.signed.state`; schema 3 is written to `metadata.release.state`, so a
compatibility migration preserves the complete source envelope without an
overwrite. Current schema 3 always wins. Required policy fails closed on a
schema-2/schema-1/raw legacy source and performs zero writes; insecure mode may
load it, write schema 3, and retain the exact source. Every schema-3 record is
cryptographically verified over its typed Item during decode and policy load.

The V1 protobuf contract adds only optional field 13
`publisher_statement_version` and optional field 14
`publisher_release_sequence`; the existing key/signature fields remain 11/12.
V2 responses contain `(version=2, sequence, key, signature)`, V1 compatibility
responses contain `(version=1, key, signature)` without sequence, and unsigned
compatibility responses contain none of the four. Every other partial or
cross-version tuple is malformed. Older enforcing clients fail V2 safely
because a V2 signature cannot verify as V1.

Client rollback state is a separate durable envelope kind 5, schema 1, at
`distd_client_rollback.state`, with payload magic `DSROLL1\0`. Records are
strictly sorted by canonical server URL bytes and then artifact ID bytes and
encode `url_len:u16 || url`, `artifact_len:u16 || artifact`, `sequence:u64`, and
`statement_identity:[32]`. URLs remain bounded by 512 bytes, artifacts by 256
bytes, records by 65,536, and the envelope by 64 MiB. Construction rejects
wrong-kind/future/truncated/trailing/oversized/noncanonical/duplicate records.

After the bounded manifest frame is decoded, one blocking worker performs
strict typed conversion, exact-key policy lookup, V2 reconstruction and
signature verification, then atomically compares and advances the durable
watermark before returning the private capability that may send possession.
A lower sequence returns rollback; an equal sequence returns success only for
the exact identity; and a higher sequence durably replaces the map and rereads
it before possession. Thus a failed transfer may retain a newer watermark, but
an exact retry remains possible and no older release can subsequently reach
possession. In explicit insecure mode, V1/unsigned responses are visibly
replayable and do not read or mutate rollback state.

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
- Server schema-2 V1 and client trust-state schema 1 remain byte-identical; the
  schema-3 V2 attestation is exactly 105 bytes including its tag: 104 bytes over
  an unsigned tag, eight bytes over a schema-2 V1 attestation, and nine bytes
  over the raw 32-byte key plus 64-byte signature. The protobuf publisher tuple
  is exactly 104 bytes for sequences below 128 and at most 113 bytes at
  `u64::MAX`, compared with 102 bytes for the complete V1 tuple.
- Watermark fault injection covers every durable replacement stage. Before a
  rename the old record remains authoritative; after rename/parent sync a
  restart observes the complete higher record. No fault permits a lower or
  equal-conflicting release to send possession, accept chunks, or start
  activation, and recovery is idempotent across two restarts.
- The ignored release references preserve raw samples and enforce: the complete
  compare/replace/fsync/reread watermark transaction is at most 30 ms p95 and
  50 ms maximum at both one and 10,000 records; the exact 10,000-revision
  schema-3 payload is 3,840,022 bytes, with canonical encode at most 10 ms p95
  and authenticated decode at most 70 ms p95 / 100 ms maximum. These are local
  reference gates rather than portable filesystem latency guarantees.

## Verification evidence

The final arm64 macOS release watermark reference used five warmups followed by
20 measured durable updates. At one record it measured 8.046 ms p95 and 8.050
ms maximum. At 10,000 records (830,004 payload bytes) it measured 17.173 ms p95
and 18.121 ms maximum. Every sample included compare, canonical encoding,
envelope integrity, file write and sync, rename, parent-directory sync, bounded
reread, decode, and exact receipt comparison. Both checked 30/50 ms gates
passed.

The exact schema-3 release reference encoded 10,000 independently signed V2
revisions into 3,840,022 bytes. Across 20 samples, canonical encode measured
7.373 ms p95 / 7.569 ms maximum and authenticated parallel decode measured
65.155 ms p95 / 73.342 ms maximum, passing the checked 10 ms encode and
70/100 ms decode gates. The metadata golden is 1,080 bytes with BLAKE3
`98d69202ad93a1edecae7366947e11ad3ab8d02cfa50f84fac8ba4a0c9adf6a1`;
the rollback payload golden is
`4c04189676314ee7f441503c1357db54db6ab23927947001030cb11415596925`.

The complete stable workspace suite passed, including 204 core, 82 server, 77
client, and 121 bench/library/CLI tests; manual references remained explicitly
ignored. Fresh product binaries additionally passed the ignored signed
file/mixed-delta/directory QUIC activation integration and the untrusted
manifest no-possession/no-activation restart integration. The nightly
all-features suite passed after replacing one pre-existing 100-yield
cleanup-thread test race with a bounded one-second deadline. The configured
strict correctness/suspicious Clippy gate, formatting, diff checks, and
`spec lint` passed; lint retained two unrelated refinement warnings and no
errors.
