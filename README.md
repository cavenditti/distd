# distd

`distd` v0.2 is an authenticated, single-origin artifact synchronizer. A root
server publishes versioned single-file or directory artifacts; clients compare
ordered BLAKE3 chunk manifests and transfer only chunks they do not possess over
QUIC.

> [!WARNING]
> This is a security-hardened research prototype, not a production update
> framework. The server authenticates publisher signatures at publication,
> and clients enforce an independent publisher allowlist before transfer.
> Rollback policy is not yet implemented.

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
| Rollback protection | Not implemented |

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

Fetch an artifact by stable ID into a client-selected relative destination:

```bash
cargo run -p distd_client -- get artifact-id local/path
```

Remote metadata never selects the client destination. Absolute paths, `..`, and
symbolic-link components are rejected by filesystem storage.

Publish through the authenticated HTTP endpoint:

```bash
curl --fail-with-body \
  -H "Authorization: Bearer $DISTD_PUBLISH_TOKEN" \
  -H 'X-Distd-Publisher-Statement-Version: 1' \
  -H "X-Distd-Publisher-Public-Key: $DISTD_PUBLISHER_PUBLIC_KEY" \
  -H "X-Distd-Publisher-Signature: $DISTD_PUBLISHER_SIGNATURE" \
  -F 'item=@./release.bin' \
  'http://127.0.0.1:3000/items?artifact_id=my-app&name=MyApp&path=server/release.bin'
```

The detached signature covers distd's canonical publisher statement for the
typed manifest and ordered chunks, not the multipart or Protobuf bytes. Private
key tooling remains intentionally outside the server. Every key referenced by
retained signed history must remain in `DISTD_PUBLISHER_PUBLIC_KEYS`; removing
one makes secure startup and sync fail closed. For isolated research only,
`DISTD_ALLOW_UNSIGNED_PUBLICATION=true` replaces the allowlist and permits the
same request without publisher headers; the server logs that insecure mode
prominently.

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

Clients verify every received chunk and the complete manifest root before
materializing a staging tree. Activation replaces the previous file or directory
with a same-filesystem rename, so shorter files and removed directory entries do
not survive an update.

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

- Multipart publication is authenticated and capped at 512 MiB, but fields are
  still buffered in memory before chunking.
- A client retains all missing chunks for an artifact until atomic activation;
  individual frames are bounded, but artifact-wide sync memory is not yet bounded.
- Signed publication authenticates the original publisher at the server, but
  client-side publisher allowlists and rollback selection remain future work.
- Server metadata and chunk storage must be backed up and restored together.
- Client UUIDs are session authorization handles; after a server restart,
  clients re-enroll using the configured enrollment secret.
- Filesystem containment rejects existing symlink components but does not claim
  protection from a hostile local process racing filesystem operations.
- No license has been selected for the repository yet.
