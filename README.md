# distd

`distd` v0.2 is an authenticated, single-origin artifact synchronizer. A root
server publishes versioned single-file or directory artifacts; clients compare
ordered BLAKE3 chunk manifests and transfer only chunks they do not possess over
QUIC.

> [!WARNING]
> This is a security-hardened research prototype, not a production update
> framework. It does not yet authenticate the software publisher or provide a
> rollback policy.

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
| Publisher/build signatures and rollback policy | Not implemented |

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
  -F 'item=@./release.bin' \
  'http://127.0.0.1:3000/items?artifact_id=my-app&name=MyApp&path=server/release.bin'
```

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

## Remaining v0.2 limitations

- Multipart publication is authenticated and capped at 512 MiB, but fields are
  still buffered in memory before chunking.
- A client retains all missing chunks for an artifact until atomic activation;
  individual frames are bounded, but artifact-wide sync memory is not yet bounded.
- Certificate pinning authenticates the distribution server, not the original
  software publisher. Signed manifests are still required for that boundary.
- Server metadata and chunk storage must be backed up and restored together.
- Client UUIDs are session authorization handles; after a server restart,
  clients re-enroll using the configured enrollment secret.
- Filesystem containment rejects existing symlink components but does not claim
  protection from a hostile local process racing filesystem operations.
- No license has been selected for the repository yet.
