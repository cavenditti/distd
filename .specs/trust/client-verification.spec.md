---
id: TASK:trust/client-verification
type: task
status: accepted
summary: >
  Fail closed on untrusted sync manifests before chunk transfer or destination
  activation while preserving the previously active artifact.
owners: [carlo]
progress: done
refines:
  - REQ:trust/publisher-authenticity#c-client
  - REQ:product/artifact-delivery#c-activation
aspects: [authenticity, activation]
assignee: carlo
eta:
blocked_by: []
---

# Client verification

## Plan

Add a client publisher policy that is independent of the persisted TLS pin,
enrollment identity, and server metadata. `[server].publisher_public_keys` is a
TOML/JSON array of 1 through 1,024 unique exact 64-character lowercase
hexadecimal raw Ed25519 public keys. Empty, missing, duplicate, uppercase,
non-hexadecimal, wrong-length, or oversized lists fail during settings
deserialization, before client trust state, storage, networking, or migration
is opened. The mutually exclusive exact
`[server].allow_unsigned_publication = true` switch is the only research
compatibility mode; neither configured is fail-closed. Publisher keys remain
explicit configuration rather than TOFU or durable client state. Debug and
startup output contains only a key count and sorted derived fingerprints, never
raw keys, signatures, or enrollment tokens.

The policy is scoped to the same canonical configured server and remains an
immutable `Arc` across enrollment, activation, reconnect, and sync. A TLS
certificate key grants no publisher authority unless its exact raw key is also
in the publisher allowlist. Removing a key prevents future synchronization
under that key but neither deletes nor relabels an already active artifact.
Explicit insecure mode admits an absent publisher tuple, but a present tuple
must still be complete, canonical, and cryptographically valid under its
carried key; it is never described as trusted.

Make the [manifest-first client flow](spec:src:client/src/server/mod.rs) a
capability transition. A private pending sync owns the raw manifest response
and continuation but cannot send possession. It can yield a private verified
sync only after all of these checks succeed:

1. decode the bounded Protobuf frame and move Protobuf-to-typed conversion to a
   blocking worker;
2. require an all-or-none exact 32-byte key and 64-byte signature tuple, and in
   secure mode require exact raw-key membership before Ed25519 work;
3. use the strict inverse chunk-profile conversion, exact hash widths, checked
   count/size conversions, canonical entries, and the shared statement bounds;
4. require the requested artifact ID, catalog artifact ID, and response
   artifact ID to agree; request the exact catalog revision and require its
   version and root hash/size to match the response;
5. reconstruct and verify the canonical publisher statement on the blocking
   worker, or reconstruct the same canonical statement without authenticity in
   explicit insecure mode.

Only the verified capability can build and send the possession bitfield or
read chunk data. Any structural, identity, policy, signature, worker, or
cancellation failure before possession explicitly resets/stops that QUIC
stream. It accepts no chunks, invokes no cache/storage mutation, and leaves the
active selector, Item, checkpoint, and destination byte-identical. A
caller-selected read-only diff-basis snapshot may be taken before the request;
it neither admits remote bytes nor changes cache accounting or active state.
The remote catalog cache is advisory and may already have changed; it is not local active
metadata. After successful verification the existing transfer integrity and
owned `spawn_blocking` activation protocol remain authoritative, so cancellation
after activation begins converges only to a complete verified old or new
revision under its existing crash rules.

Surface a distinct non-secret publisher-authenticity error with stable
categories for unsigned, malformed, untrusted, identity-mismatched, and invalid
statement/signature responses. Remote name, description, and deployment path
remain outside the signed statement; the destination stays caller-selected.
Rollback/downgrade selection, publisher TOFU, key rotation/revocation UX,
transparency logging, and trusting unsigned artifacts in secure mode remain
non-goals.

## Acceptance

- A valid independently signed single-file and directory artifact completes a
  cold fetch and a mixed reused/received delta update under an allowed
  publisher key. Every strict chunk algorithm, including the detected bzip2
  profile, round-trips through the same verification boundary.
- Unsigned, unknown-key, malformed, and tampered manifest fields fail before
  the possession message is sent; no chunks are accepted, the prior
  destination remains byte-identical, and local active metadata is unchanged.
- Requested/catalog/response artifact, version, and root mismatches fail at the
  same boundary. Partial tuple, key/signature length, hash/count/size,
  algorithm-profile, entry order/path/range, cross-artifact reuse, wrong key,
  and covered-field mutation tests all observe zero possession and zero
  storage/cache mutation or activation calls. A caller-selected read-only
  diff-basis snapshot may be taken before the request; it neither admits remote
  bytes nor changes cache accounting or active state.
- Reusing the pinned TLS server key as a publisher key fails unless that exact
  public key is separately present in the publisher allowlist. Configuration
  tests cover 1/1,024 accepted keys and every missing, malformed, duplicate,
  ambiguous, and 1,025-key failure without opening durable state.
- Explicit insecure mode accepts a canonical unsigned manifest and a valid
  complete signed tuple, logs its insecurity, and still rejects partial or
  invalid signed tuples. Secure mode never silently falls back.
- Rejected and cancelled syncs preserve the exact old destination, active
  handle/Item, FsStorage durable bytes, cache accounting, and restart result.
  Transfer corruption and every existing ordinary/indeterminate activation
  boundary preserve the same verified-old-or-verified-new invariant across two
  restarts.
- Same-destination valid/invalid concurrency cannot let the invalid contender
  consume or activate bytes; independent destinations remain concurrent; a
  catalog refresh cannot change the immutable snapshot used by an in-flight
  exact-version sync.
- Maximum-manifest frame decode, strict typed conversion, and canonical
  verification run outside Tokio workers in one blocking task. Event-loop drift
  and blocking-pool dispatch coordination each remain at most 1 ms p95 and 5 ms
  p99; the observed maximum is reported but not treated as a portable scheduler
  guarantee. The CPU duration inside the complete pre-possession worker, including
  bitfield construction but excluding the separately reported dispatch delay,
  is at most 250 us p95 for 1/64 chunks, 2 ms for 4,096, and 20 ms p95/35 ms
  maximum for 65,536-entry file and directory manifests.
  The canonical signature reference budgets remain 10
  us fingerprint p95, 100 us construction/verification for 1/64 chunks, 1 ms
   for 4,096, and 10/15 ms respectively for 65,536. Verification uses one canonical
  statement allocation and no second encode or deep manifest clone.
- No async server-state lock is held across network I/O or blocking work;
  metadata/fetch lock wait is at most 1 ms p95 and independent artifact syncs
  may progress concurrently. The allowlist is parsed once at startup in at
  most 5 ms p95/10 ms maximum, then exact membership is expected O(1).
- Client configuration, pure capability, real QUIC no-possession, cold/delta
  FsStorage, cancellation, staging/restart, release reference, and the full
  stable/nightly documented validation suites pass.
