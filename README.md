# distd

`distd` v0.2 is an authenticated, single-origin artifact synchronizer. A root
server publishes versioned single-file or directory artifacts; clients compare
ordered BLAKE3 chunk manifests and transfer only chunks they do not possess over
QUIC.

> [!WARNING]
> This is a security-hardened research prototype, not a production update
> framework. The server authenticates publisher signatures at publication,
> clients enforce an independent publisher allowlist before transfer, and a
> durable per-authority release watermark rejects publisher-sequence rollback.

## What v0.2 implements

| Capability | Status |
| --- | --- |
| QUIC artifact synchronization | Implemented |
| Fixed and content-defined chunk profiles | Implemented |
| Possession-bitfield delta transfer | Implemented |
| Single-file and directory manifests | Implemented |
| Immutable artifact version lookup | Implemented |
| Persistent server TLS identity and metadata | Implemented |
| Explicit TLS certificate pinning | Default client policy |
| Token-gated client enrollment and HTTP administration | Implemented |
| Verified staging and atomic destination replacement | Implemented |
| P2P distribution, swarms, or a distribution tree | Not implemented |
| Publisher signatures at the server publication boundary | Implemented |
| Client publisher allowlist and signature verification | Implemented |
| Durable publisher-sequence rollback protection | Implemented |
| Configured client missing-payload admission | Implemented |

The term “PPSPP-style” in the code refers only to the manifest → possession
bitfield → missing-chunk exchange. There is currently no peer discovery,
multi-source scheduling, HAVE propagation, or swarm protocol.

## Requirements

The repository pins Rust 1.96.0 for normal development. The optional
`nightly-bench` feature requires a current nightly toolchain.

## Secure local setup

The server refuses to start without an enrollment token. HTTP publication and
administrative routes also remain unavailable unless their tokens are set.

```bash
export DISTD_ENROLLMENT_TOKEN="$(openssl rand -hex 32)"
export DISTD_PUBLISH_TOKEN="$(openssl rand -hex 32)"
export DISTD_ADMIN_TOKEN="$(openssl rand -hex 32)"
export DISTD_PUBLISHER_PUBLIC_KEY='<64-lowercase-hex Ed25519 public key>'
export DISTD_PUBLISHER_PUBLIC_KEYS="$DISTD_PUBLISHER_PUBLIC_KEY"
export DISTD_PUBLISHER_SIGNATURE='<128-lowercase-hex detached Ed25519 signature>'
export DISTD_SERVER_STATE_DIR="$PWD/.distd-server-state"
export DISTD_STORAGE=fs
export DISTD_STORAGE_ROOT="$PWD/.distd-server-storage"
export DISTD_PUBLICATION_MEMORY_MIB=512
export DISTD_PUBLICATION_BODY_TIMEOUT_SECS=600
export DISTD_MAX_REGISTERED_CLIENTS=4096
cargo run -p distd_server
```

On startup the server logs the BLAKE3 fingerprint of its persistent QUIC
certificate. Copy that value into `server.certificate_fingerprint` in
`ClientSettings.toml`, along with the enrollment token. Explicit pinning is
required by default. `allow_tofu = true` exists for isolated development, but
the first connection is then vulnerable to interception.

The client independently requires the exact publisher authority in
`server.publisher_public_keys`. Copy the publisher's raw 32-byte Ed25519 public
key as 64 lowercase hexadecimal characters; the QUIC certificate pin and
`allow_tofu` never authorize artifact content. For isolated research servers
only, `allow_unsigned_publication = true` may replace the publisher-key list.
The two settings are mutually exclusive, and omitting both fails closed before
the client connects or changes local artifact state.

`client.max_sync_memory_mib` is the backward-compatible name of the
missing-payload admission limit and defaults to 512 MiB (accepted range: 1
through 1,024 MiB). The client computes the exact missing-byte sum from the
verified manifest and local possession basis, then rejects over-budget releases
before advancing rollback state or sending possession. Admitted payload is not
retained in RAM: verified chunks flow through a 4 MiB frame, a one-batch
backpressure channel, and a transfer-scoped disk pack. The limit therefore
controls transfer and temporary-disk exposure rather than heap allocation.

Fetch an artifact by stable ID into a client-selected relative destination:

```bash
cargo run -p distd_client -- get artifact-id local/path
```

Remote metadata never selects the client destination. Absolute paths, `..`, and
symbolic-link components are rejected by filesystem storage.

`get --lazy` performs the same publisher and rollback verification but fetches
no payload. It records the revision under `.distd-activation-v1/lazy/` and
prints the path of a content-addressed composefs EROFS image in
`.distd-activation-v1/composefs/`. Every file in the image redirects to its
whole-file blob in `.distd-activation-v1/blobs`, which on-demand hydration
fills in later:

```bash
cargo run -p distd_client -- get --lazy artifact-id local/path
```

A lazy activation is independent of an eager one for the same destination.
Images use `0444` for regular files, `0555` for executable files and
directories, and `S_IFLNK` inodes for symbolic links. Eager `get` refuses
artifacts with executable or symlink entries and points to `--lazy`: eager
activation hard-links files with shared CAS blobs, so a per-entry mode cannot
be represented safely.

Publish through the authenticated HTTP endpoint:

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $DISTD_PUBLISH_TOKEN" \
  -H 'X-Distd-Publisher-Statement-Version: 2' \
  -H "X-Distd-Publisher-Release-Sequence: $DISTD_PUBLISHER_RELEASE_SEQUENCE" \
  -H "X-Distd-Publisher-Public-Key: $DISTD_PUBLISHER_PUBLIC_KEY" \
  -H "X-Distd-Publisher-Signature: $DISTD_PUBLISHER_SIGNATURE" \
  -F 'item=@./release.bin' \
  'http://127.0.0.1:3000/items?artifact_id=my-app&name=MyApp&path=server/release.bin'
```

The detached signature covers distd's canonical V2 publisher release statement
for the publisher-controlled sequence, typed manifest, and ordered chunks—not
the multipart or Protobuf bytes.

Directory releases may mark entries as executable files (multipart field
`item-executable`) or symbolic links (field `item-symlink`, whose body is the
UTF-8 target, such as `/nix/store/…-hello/bin/hello`). A symlink is an empty
entry covering one empty chunk; its target must be nonempty NFC UTF-8 of at
most 4,095 bytes without NUL. Such releases are signed as a V3 statement, which
adds each entry's kind and target, and the request must declare
`X-Distd-Publisher-Statement-Version: 3`. Manifests with only regular files
keep their exact V2 statement and every existing persisted encoding. Unsigned
and replayable V1 publications cannot carry entry kinds, so older clients
always reject them as an unknown statement version. Sequences are monotonic per artifact. Private
key tooling remains intentionally outside the server. Every key referenced by
retained signed history must remain in `DISTD_PUBLISHER_PUBLIC_KEYS`; removing
one makes secure startup and sync fail closed. For isolated research only,
`DISTD_ALLOW_UNSIGNED_PUBLICATION=true` replaces the allowlist and permits the
same request without publisher headers; the server logs that insecure mode
prominently.

`DISTD_PUBLICATION_MEMORY_MIB` bounds aggregate declared multipart bodies held
by authenticated publications and defaults to 512 MiB (accepted range: 1
through 2,048 MiB). `POST /items` requires `Content-Length`; a request that
cannot fit returns 413, while temporary aggregate exhaustion fails fast with
429 before multipart fields are buffered. The reservation remains held through
durable publication and is released on every completion or cancellation path.
`DISTD_PUBLICATION_BODY_TIMEOUT_SECS` applies an absolute body-transfer
deadline after admission (default 600 seconds; accepted range: 1 through
86,400). A stalled or trickled multipart body returns 408 and releases its
partial buffers and reservation before storage preparation begins; the
deadline ends once extraction completes and never cancels an owned durable
publication.

`DISTD_MAX_REGISTERED_CLIENTS` bounds process-lifetime client enrollment state
and defaults to 4,096 entries (accepted range: 1 through 65,536). Client names
must contain 1 through 256 UTF-8 bytes. Exact UUID reuse remains available at
capacity, while a distinct registration fails before insertion.

Reusing `artifact_id` creates a new immutable version only when its content or
relevant metadata changes. `name` and `path` may change without changing the
artifact identity.

## Architecture

- `distd_core`: chunking, manifests, hash trees, bounded framing, and storage.
- `distd_server`: authenticated REST publication, persistent metadata, and QUIC
  synchronization.
- `distd_client`: pinned server connection, possession negotiation, verification,
  and atomic local activation.
- `distd_bench`: isolated experimental harness and QUIC network shaper.

Clients verify each received chunk while appending it to a transfer-scoped pack,
then re-verify the complete manifest root while materializing an immutable
revision. Only the completed revision can enter the existing atomic activation
transaction. Activation replaces the previous file or directory with a
same-filesystem rename, so shorter files and removed directory entries do not
survive an update. Aborted transfers remove their pack, and startup removes
well-formed packs left by a process crash.

### Random-access blob reads

`distd_core::blob::BlobReader` exposes a file through `std::io::Read`,
`std::io::Seek`, and `read_at(offset, buffer)` without assembling an output file.
Use `BlobReader::new(&storage, &item)` for a single-file artifact or
`BlobReader::open_file(&storage, &item, "relative/path")` for a directory entry.
`read_at` preserves the stream cursor; offsets are relative to the selected file.

The reader indexes actual chunk sizes, supports fixed and content-defined
profiles, and verifies each touched chunk's length and BLAKE3 hash before
returning bytes. It retains one verified chunk plus an offset index; backend
caches have their own budgets. HashMap, filesystem, redb, and standalone CAS
storage implement the `BlobChunkSource` interface. Reads use chunk identities
directly and do not require a complete stored tree.

Unavailable chunks return `NotFound` with a typed `MissingChunk` containing the
hash and size, allowing a caller to fetch the chunk and retry. Reads that reach
a missing or corrupt chunk after copying valid bytes return that verified
prefix; the next read at the failing offset reports the error. Opening does not
require all payloads to be resident. Network fetching, lazy mounting, Snix blob
identities, and activation before transfer completion remain future work.
Callers must authenticate the revision with the existing publisher and rollback
policies before using this interface; metadata consistency and chunk integrity
checks alone do not establish publisher trust.

### QUIC wire contract

Every operation stream begins with the fixed 12-byte V1 preface
`DSWP 0001 0001 0001 0000` before the operation byte. The fields are the
preface format, operation-layer version, Protobuf-contract version, and a
reserved zero, encoded as big-endian `u16` values. V1 operation identifiers are
`1=Register`, `2=Fetch`, and `3=Sync`; all other byte values are reserved.

The only supported compatibility row is operation V1 with Protobuf V1, in both
directions. A complete malformed or unsupported declaration receives a fixed
12-byte `DSWR` rejection before the server reads UUID or operation payload. The
successful response frames remain byte-identical to the pre-preface V1
messages. Protobuf field numbers, oneof tags, and enum values are never reused;
removed identifiers must be reserved, and semantic changes that an older reader
cannot safely ignore require a Protobuf-contract version bump.

## Validation

```bash
cargo fmt --all -- --check
cargo check --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings -A clippy::all -A clippy::pedantic -A clippy::nursery -A clippy::restriction -D clippy::correctness -D clippy::suspicious
cargo test --workspace --locked
cargo +nightly test --workspace --all-features --locked
```

CI runs the same stable checks and the nightly feature suite.

## Benchmark limits

The harness gives each run isolated client/server state and reports QUIC UDP
bytes rather than decoded payload size. Its userspace shaper applies only to the
distd QUIC path; local `rsync` and other runners are not automatically placed
behind an equivalent network. Results across unlike transport paths must not be
presented as controlled network comparisons.

“Resume” remains a planned workload. A complete transfer is not labeled as a
resume measurement.

The redb storage microbenchmark checks committed bytes after reopening each
database, then measures tree reconstruction, indexed reads, and appending into
a preallocated transfer buffer. It covers unique and repeated 16 MiB payloads
and 2,048 small files, with five fresh databases per workload:

```bash
cargo run --release -p distd_core --features redb --locked --example redb_storage_bench
```

Its JSON lines report elapsed time and allocation traffic (requested bytes,
not peak memory). These are local storage measurements; they do not measure
QUIC transfer speed or compare distd with other distribution systems.
The [2026-10-29 storage comparison](docs/benchmarks/redb-storage-2026-10-29.json)
records baseline and candidate samples, allocation counts, source identities,
and validation results.

### Frozen QUIC Fetch acceptance workflow

`quic-fetch-acceptance` is a fail-closed acceptance driver for one preregistered
comparison: the frozen baseline and candidate executables running
`quic::tests::manual_quic_authenticated_fetch_handler_5000`. It is not a general
benchmark launcher.

The authority boundary is deliberately split into four steps:

1. `capture-source` records the current repository identity without running a
   product executable.
2. `compose-request` consumes that capture, the two complete source bundles,
   their supporting build inputs, the reviewed patch, and the recovered legacy
   receipt bytes. It publishes one canonical, read-only request without running
   a product executable.
3. `prepare` accepts only the exact canonical request bytes. It repeats the
   shared provenance checks, rehashes every input, validates the current product
   source projection, and copies the request, source bundles, legacy anchors,
   executables, and supporting inputs into an immutable plan tree. Only a
   `PREPARED` plan may cross the execution boundary.
4. `execute` and `verify` both authenticate the sealed plan with the same
   production provenance policy. Execution repeats live input, repository, and
   environment checks once before the fixed series, then verifies only the
   selected executable's descriptor, inode, and hash immediately around each
   spawn. This avoids full source-tree I/O between timed children. Verification
   never reruns a product process.

The normal order is therefore:

```text
capture-source -> compose-request -> prepare -> execute -> verify
```

For example, with every destination absent before the command starts:

```sh
./target/release/quic-fetch-acceptance capture-source \
  --repository /absolute/path/to/distd \
  --output /absolute/secure/capture \
  --task-spec /absolute/path/to/distd/.specs/performance/run-final-source-quic-fetch-acceptance.spec.md

./target/release/quic-fetch-acceptance compose-request \
  --input /absolute/secure/compose-input.v3.json \
  --output /absolute/secure/request.json

./target/release/quic-fetch-acceptance prepare \
  --request /absolute/secure/request.json \
  --plan /absolute/secure/plan \
  --blocked-evidence /absolute/secure/BLOCKED.json

# Authority stop: continue only when prepare printed PREPARED for this plan.
./target/release/quic-fetch-acceptance execute \
  --plan /absolute/secure/plan/plan.json

./target/release/quic-fetch-acceptance verify \
  --plan /absolute/secure/plan/plan.json \
  --evidence /absolute/secure/execution-evidence
```

`capture-source --output` is an absent directory outside the repository.
`compose-request --output`, `prepare --plan`, `--blocked-evidence`, and the
descriptor's `evidence_root` must also be absent; output roots may not overlap
one another or any input parent. The v3 compose descriptor is canonical
one-line JSON with this expanded shape (each `FileInput` is an absolute `path`
plus its lowercase `sha256`):

```text
schema: distd.quic.fetch.compose-input.v3
repository_capture: FileInput for capture/repository.json
baseline, candidate:
  executable, provenance_receipt, source_snapshot, source_manifest,
  source_bundle, benchmark_source, cargo_lock, workspace_manifest,
  package_manifest, cargo_config?, rustc_verbose, cargo_verbose,
  build_environment, profile_identity: FileInput
  legacy_evidence:
    primary, secondary: FileInput
    environment?, preregistration?, build_result?: FileInput
reviewed_patch_manifest: FileInput
blocked_evidence, evidence_root, output: absolute absent paths
```

Nullable legacy fields are role-specific: production requires the exact
recovered baseline environment, preregistration, and build-result payloads and
requires all three to be absent for the candidate. The descriptor is not an
acceptance request. Only `compose-request` may publish `request.json`; there is
no supported hand-authored-request path into `prepare`.

The driver rejects an individual provenance input above 64 MiB. A source
bundle is additionally limited to 4,096 entries, 8 MiB per decoded entry, and
32 MiB of aggregate decoded contents.

The CLI terminal vocabulary is `PREPARED`, `BLOCKED`, `PASS`, `FAIL`, and
`INVALID`. Each terminal line names its evidence path. Exit status `0` means a
successful non-executing preparation step or `PASS`; `2` means an authenticated
`FAIL`; `3` means `BLOCKED` before product execution; and `1` means `INVALID`
input, plan, or execution evidence. Managed signals are re-raised after cleanup
so shells retain the corresponding signal exit status.

The recovered provenance directory contains byte-exact payloads recovered from
the durable session log: the baseline executable identity, source-status,
environment, preregistration, and build-result receipts, and the candidate
identity and build-command receipts. Their original files and inode identities
are gone. The newer structured wrapper is therefore evidence-derived, not an
original historical receipt. Its fixed policy anchors the exact recovered
payload hashes; it does not treat caller-authored metadata as historical
authority. The recovered environment receipt supports only the facts present in
those bytes; the original build-process environment itself remains unavailable.

The candidate source state is a separate question from the executable: its
historical tracked tree can be materialized deterministically from commit
`a17d501` and the reviewed `4dbe52c..a17d501` transition, then checked against
the frozen lockfile and QUIC-source identities. The canonical 45-entry
build-source bundle is independently anchored by SHA-256
`fbbb585bfb4404c9abfb01e807451045becf669361f9275c7450b21e88e87c13`.
Recoverable source bytes do not authorize rebuilding or stand in for the
missing executable bytes.

No acceptance result may be produced by rebuilding an arm, substituting a
different executable, or accepting a merely self-consistent provenance bundle.
The exact candidate executable with SHA-256
`6cb669536efea5b3055c6f4487624b96abc136cc5d036cbfaae672a651fcb327`
is currently absent. Independently, the exact baseline source bundle has not
been recovered and sealed as a production code anchor. Production composition
therefore fails closed before publication, and the final comparison remains
`BLOCKED` even if only one of those gaps is closed. Execution requires both the
exact candidate bytes and the independently anchored baseline source bundle;
reconstruction, rebuilding, substitution, or a hash-policy exception is not a
path around either requirement.

## Remaining v0.2 limitations

- Multipart publication fields are still buffered before chunking, but each
  request remains capped at 512 MiB and aggregate declared bodies are bounded
  by `DISTD_PUBLICATION_MEMORY_MIB`.
- Transfer packs are crash-safe but not resumable across process restarts; a
  restarted client removes the incomplete pack and requests its missing chunks
  again.
- Server metadata and chunk storage must be backed up and restored together.
- Client UUIDs are session authorization handles; after a server restart,
  clients re-enroll using the configured enrollment secret.
- Filesystem containment rejects existing symlink components but does not claim
  protection from a hostile local process racing filesystem operations.
- No license has been selected for the repository yet.
