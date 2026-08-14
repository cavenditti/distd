---
id: TASK:trust/signed-publication
type: task
status: accepted
summary: >
  Require, verify, persist, and serve publisher signatures at the publication
  boundary before a revision becomes visible.
owners: [carlo]
progress: done
refines:
  - REQ:trust/publisher-authenticity#c-publication
  - REQ:trust/publisher-authenticity#c-propagation
aspects: [publication, propagation]
assignee: carlo
eta:
blocked_by: [TASK:trust/signature-envelope]
---

# Signed publication

## Plan

Configure an explicit server allowlist of exact publisher Ed25519 public keys.
The publisher key and its detached signature authorize only the canonical
statement frozen by the [signature-envelope task](spec:TASK:trust/signature-envelope).
The HTTP bearer token authorizes use of the publication endpoint but is not a
publisher authority; the server TLS key, enrollment secret, fingerprints,
artifact name/path, metadata store, storage backend, Protobuf, and revision
allocator are likewise non-authoritative. Private signing keys never enter the
server. Replay, rollback selection, key rotation/revocation policy, private-key
tooling, and orphan-chunk garbage collection remain non-goals.

Production configuration uses `DISTD_PUBLISHER_PUBLIC_KEYS`, a comma-separated
list of 1 through 1,024 unique exact 64-character lowercase hexadecimal raw
public keys. ASCII whitespace is allowed only around complete entries. Empty,
duplicate, uppercase, non-hexadecimal, wrong-length, non-UTF-8, or oversized
lists fail before metadata migration or listeners start. Trust compares exact
32-byte keys; BLAKE3 fingerprints are derived only for safe display. An empty
allowlist never enables unsigned operation. Research compatibility requires
the separate exact `DISTD_ALLOW_UNSIGNED_PUBLICATION=true` switch, is mutually
exclusive with the allowlist, logs a prominent warning, and is the only mode
that may load, serve, or create unsigned revisions.

The authenticated [HTTP publication boundary](spec:src:server/src/rest_api.rs)
accepts exactly one of each singleton header:

- `X-Distd-Publisher-Statement-Version: 1`, encoded as that exact canonical
  decimal text;
- `X-Distd-Publisher-Public-Key`, encoded as 64 lowercase hexadecimal
  characters;
- `X-Distd-Publisher-Signature`, encoded as 128 lowercase hexadecimal
  characters.

All three headers are required in publisher-enforcing mode. They are either all
absent or all present in explicitly insecure mode; partial tuples are always
malformed. Header parsing and immutable allowlist precheck occur before reading
the multipart body, while the existing bearer middleware runs first. Missing,
duplicate, malformed, or unknown-version headers return `400`; an unknown key
or cryptographically invalid/mismatched signature returns the same `403`;
exact-content/different-envelope conflicts return `409`; poisoned publication
state returns `503`; body limits and unrelated durable/storage failures retain
their existing status. Error and log text never includes bearer/enrollment
tokens, raw configured keys, or signatures, and HTTP tracing continues to
exclude headers.

Represent every latest and historical immutable revision as one
`PublishedRevision { item, attestation }` value. The attestation is either a
byte-exact V1 `PublisherSignatureEnvelope` or an explicit insecure unsigned
marker; it is never a parallel map and never stores a claimed fingerprint.
Read-only accessors project the existing `Item` API where storage and public
metadata require it. Nonempty feeds may reference only byte-identical existing
published revisions; an empty startup feed is removed or remains harmless.

Single-file and directory publication use the following owned coordinator:

1. validate external paths using the platform-independent signing grammar,
   canonical UTF-8 byte ordering, and prepare the server-computed typed Item on
   a blocking worker without storage mutation;
2. acquire the existing per-artifact publication lock, require writable
   metadata, and recheck the exact publisher key against the immutable policy;
3. reconstruct and verify the canonical statement on a blocking worker while
   retaining the artifact lock, before idempotence, revision selection, or
   storage commit;
4. compare the current signed content and attestation: an exact relevant Item
   and byte-identical envelope is idempotent, while the same signed statement
   under a different envelope is an immutable-attribution conflict;
5. choose the next revision with checked arithmetic, durably commit immutable
   storage, insert one Item-plus-attestation value into the candidate metadata,
   durably replace and reread that complete snapshot, then publish memory;
6. preserve the existing cleanup and poison semantics on metadata failure.

The coordinator survives caller cancellation. Validation occurs on every
request, including idempotent repeats, and invalid authorization cannot reach
storage mutation. The server-assigned revision remains unsigned, so the final
revision is applied only after successful verification. Post-rename metadata
failure remains indeterminate and fail-closed: live memory stays old, while
restart may expose only a complete old or complete new signed revision.

Keep `DurableStateKind::ServerPublication = 3`, introduce payload schema 2 and
magic `DSPUBV2\0`, and retain the 64 MiB payload and existing collection caps.
Each sorted history record encodes its revision key, the frozen schema-1 Item
layout, an attestation tag (`0` explicitly unsigned, `1` signed V1), and for tag
1 the exact raw public key `[u8;32]` and signature `[u8;64]`. The tag supplies
the frozen V1 envelope/statement version; fingerprints and redundant envelope
domain bytes are not persisted. Unknown tags, trailing bytes, invalid
Item/envelope pairs, and unsigned records in secure mode fail closed. Latest
remains derived from the greatest history key. Every signed record is
cryptographically revalidated against its own Item on decode and its exact raw
public key must remain in the current secure allowlist. Publisher rotation
therefore retains every key still referenced by history; removing one fails
startup and sync closed. Revocation and rotation UX remain out of scope.

Use `metadata.signed.state` as the schema-2 current file. Its presence
suppresses every fallback. The existing schema-1 `metadata.state` becomes a
distinct retained legacy-envelope source, followed only when schema 2 is
absent; the exact raw-Bitcode `metadata.bitcode` layouts remain the final
fallback. Secure startup rejects either unsigned source without creating or
rewriting schema 2. Explicitly insecure migration marks every legacy revision
unsigned, atomically writes/rereads schema 2, and retains the selected source
byte-exact. Corrupt, future, ambiguous, symlink, special, truncated, trailing,
or oversized state never falls through to an older source.

Append `publisher_public_key = 11` and `publisher_signature = 12` to
`ManifestResponse` without renumbering the existing schema. Those fields are
the V1 wire tuple; a future signature format must use new fields rather than
reinterpret them. Signed records emit exactly 32 key bytes and exactly 64
signature bytes from the selected historical value; unsigned insecure records
emit the all-default tuple. Partial tuples never arise. Protobuf carries the
typed data and detached signature but is never signed. Add a fallible exact inverse of
`ChunkAlgorithm::to_proto` which rejects missing/unknown/defaulted profiles,
unsupported versions, noncanonical unused fields, nominal disagreement, and
all values invalid under statement V1; checked chunk-size conversion replaces
the existing truncating cast. Client enforcement remains the dependent task,
but fixtures and server egress already use the strict, representable contract.

## Acceptance

- Missing, malformed, unknown-key, mismatched-artifact, and invalid signatures
  return their frozen non-success response before storage commit and leave
  latest/version metadata and the durable schema-2 file byte-for-byte unchanged
  across restart. Bearer rejection precedes multipart processing and the exact
  body-size boundary remains effective.
- A valid signature produces exactly one immutable visible revision; an
  idempotent repeat does not create a new version, and stored key/signature
  bytes round-trip unchanged through persistence and manifest sync.
- Independently signed single-file, directory, and nondefault bzip2-profile
  fixtures publish successfully. Covered-field mutations, wrong keys,
  cross-artifact signature reuse, partial tuples, and different envelopes over
  the same signed content fail without substituting historical attribution.
- Schema-2 nonempty goldens freeze multiple artifacts, revisions, and publisher
  keys; logical insertion-order permutations encode identically. Malformed
  tags/lengths/envelopes, swapped Item/envelope pairs, future schemas, and
  current/history disagreement fail closed. Removing a key still referenced by
  signed history makes secure startup and sync fail closed. Secure schema-1
  startup performs zero writes; explicit insecure migrations retain exact
  sources and restart with explicit unsigned markers.
- Latest and every exact-version QUIC query return their own byte-identical
  stored key/signature, never the latest envelope. Insecure unsigned responses
  contain an all-default publisher tuple; signed tuples are complete.
- Fault injection across every metadata replacement stage proves that ordinary
  failures retain exact old signed state and indeterminate failures restart as
  exact old or complete signed new state, never an Item without its
  attestation. Filesystem and redb/default reconciliation remain covered.
- Cancellation and concurrency tests prove signature validation,
  envelope-aware idempotence, and version advancement occur under the existing
  publication serialization boundary. Invalid contenders consume no version;
  duplicate races produce one revision; different artifacts lose neither
  signed value.
- Strict transport properties prove `try_from_proto(to_proto(x)) == x` for all
  algorithms and reject every used/unused-field mutation, absent profiles,
  unknown tags/versions, aliases, clamps, overflow, and incomplete publisher
  tuples.
- Existing bearer-token and payload-size controls remain effective, focused
  server integration tests pass, and the full documented validation suite
  passes.
- Release reference gates retain the canonical signature budgets: fingerprint
  p95 at most 10 us; construction/verification p95 at most 100 us for 1/64
  chunks, 1 ms for 4,096, and 10/15 ms respectively for 65,536. Added valid
  signed-publication work is at most 20 ms p95; invalid authorization performs
  zero storage/metadata commits. Worker-owned temporary allocation is at most
  the exact canonical statement plus 1 MiB and one prepared Item, with no
  second statement encode or deep Item clone during verification. The
  API-required second persistent Item owner for the returned value and live
  metadata is created on the storage worker, never on a Tokio worker.
- The Protobuf delta is exactly 100 bytes for a populated key/signature pair,
  and schema 2 adds at most 97 bytes per immutable revision. At 10,000
  one-revision artifacts, canonical encode p95 is at most 10 ms,
  authenticated reread/decode is at most 70 ms p95 and 100 ms maximum, and the
  full one-write/one-file-sync/rename/one-directory-sync/reread metadata commit
  is at most 100 ms p95 and 120 ms maximum on the disclosed local APFS
  reference. The 64 MiB hard cap remains a bounded correctness gate rather
  than a maximum-size latency promise.
- Preparation, canonical verification, directory sorting, storage and metadata
  I/O, and maximum manifest-response construction remain off Tokio workers.
  Async coordination/event-loop drift is at most 1 ms p95 and 5 ms maximum;
  unrelated artifacts may verify/store concurrently, while only the existing
  final metadata replacement serializes. An exact idempotent repeat performs
  zero storage and metadata writes.
