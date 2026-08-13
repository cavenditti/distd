---
id: TASK:performance/remediate-quic-fetch-artifact-provenance
type: task
status: accepted
summary: Replace opaque QUIC Fetch artifact labels with fail-closed structured provenance before preparing the final-source experiment.
owners: [carlo]
progress: done
refines:
  - REQ:performance/reproducible-acceptance-benchmarks#c-plan
  - REQ:performance/reproducible-acceptance-benchmarks#c-preflight
  - REQ:performance/reproducible-acceptance-benchmarks#c-evidence
assignee: carlo
eta:
blocked_by: []
aspects: [plan, preflight, evidence, provenance]
---

# Remediate Quic Fetch artifact provenance

## Plan

Replace the request and plan v1 artifact identity, which accepted an arbitrary
nonempty build label and opaque source-receipt bytes, and the rejected
self-consistent v2 wrapper with a versioned v3
contract in the [acceptance model](spec:src:bench/src/quic_fetch_acceptance/model.rs).
Each arm binds canonical executable and evidence files to structured
provenance: artifact role and byte origin, an evidence-derived receipt status,
historical HEAD plus tracked source snapshot and role-specific legacy source
identity, a canonical complete source bundle, benchmark source,
dependency lock plus workspace and package manifests and optional Cargo
configuration, exact Rust toolchain and target with verbose `rustc`/`cargo`
receipts, release-profile identity and its receipt, feature set, command argv,
and normalized explicit command environment overrides. The baseline role-specific
legacy source identity is its recovered source-status payload; the candidate's
is its recovered identity payload, while its second legacy payload is the build
command. The source bundle contains sorted safe repository-relative paths,
Git-compatible modes, and the complete bytes for each entry. The source
manifest MUST be derived from those bytes, and the reviewed
baseline-to-candidate patch MUST be derived mechanically from the two validated
bundles rather than accepted as a caller-authored set of transitions. Every
claimed digest MUST have a corresponding `FileInput` whose bytes preparation
and verification rehash.
No historical Git tree OID is claimed: the recovered identity is HEAD plus the
independently anchored tracked diff, and the source bundle is a build-source
closure rather than a complete Git tree.

Add a strictly non-product request composer and migrate preparation, execution,
audit, and deterministic fixtures to validate and retain the entire v3 model.
The current recovery policy is compiled independently of caller input. It
accepts only `recovered_frozen` executables with baseline SHA-256
`d60c56edf574a9980b3ba9e7572f9da95ca80260e530781495bdff4604979478`
and candidate SHA-256
`6cb669536efea5b3055c6f4487624b96abc136cc5d036cbfaae672a651fcb327`,
and only an `evidence_derived` structured wrapper anchored to exact recovered
legacy payloads. The baseline anchors are:

- baseline executable identity SHA-256
  `098a8af95d9dfae1b07a73939928f2d57c8f951fa8066d32c5776813b0cf8bf9`;
- baseline source status SHA-256
  `fcecacdd6f0f71a3b515616ecb144efd91ade2d09f10428b2c24bac4ba309941`;
- baseline captured environment SHA-256
  `53e526b066818d82355edd6f7dac54f43a80b0e13c43fdead520cf38b014f3ac`;
- baseline preregistration SHA-256
  `9fc81d187c3c9c3a0931106ae58a35c90a90efa7331bc444b02b71fb31f8725a`;
- baseline build result SHA-256
  `7f518a88b4b47c79d12b9090d285665aa53a6327ebde8efb34dfcd2bf5b96c99`.

The candidate anchors are:

- candidate identity SHA-256
  `90ae2c529fb6cd0bb959d485eefa958728a0cd81b3d888b3d2b334a2008c2330`;
- candidate build command SHA-256
  `98a55d72755da6e78cca11163f09b20812d0297c185830573de8474fa6f60eaa`.

The wrapper MUST disclose that those payloads were recovered byte-for-byte
from durable session evidence while their original file paths and inode
identities no longer exist. It MUST NOT describe itself as an original receipt.
These named legacy records are sealed by the corrected read-only recovery bundle
manifest SHA-256
`ab2d154eeef3a2f1423f2ba480d7201d159ddf82a77a6e34059e19d8a089bbb4`;
the manifest is a container integrity receipt, not a substitute for validating
each independently anchored payload.
Rebuilt, exactly reproduced, substituted, reconstructed-with-authority, or
caller-selected artifacts and policies remain representable only for a future
explicitly approved protocol and MUST be rejected for this experiment.

The source evidence is additionally pinned to baseline tracked-diff SHA-256
`246c8ad9efecff0267b72b50f2aabff8f3742139c587c4e64478757fe60aab91`,
candidate tracked-diff SHA-256
`fb536d79314f0b2d6632c0c7c3ba19586028b0e72efc547e540751bd1e64ffcf`,
historical lock SHA-256
`ad953daf188f5f11c633a7efb8a8f548310a44af9b9b5893872058db8bfcd356`,
and candidate QUIC source SHA-256
`b39c61c2592770ee50a504ce2316135a38c527c343d93862d6b7ad53da4393db`.
Exact equivalence inputs additionally anchor the structured toolchain claims:
Cargo verbose receipt SHA-256
`9db411a77dc69b743e7aa51da9aafff8b31e7aaf1afc54ae1fdb5635fc8dc7cd`,
Rustc verbose receipt SHA-256
`aea7c77c2b0d24444d07240e4d785e4ba73b6c9e786748af2ddc22bbf4c104bf`.
Both arm bundles use those exact independently anchored equivalence receipts;
caller-authored substitute verbose output is rejected.
The baseline captured-environment payload above independently supports the
toolchain release/commit/host,
historical lock, absence of workspace Cargo configuration, host conditions,
and release-profile semantics. The baseline preregistration records the literal
command `cargo test --profile release -p distd_server --no-run`; the build-result
payload independently confirms that same command exited successfully, produced
the exact baseline artifact in optimized release mode, and did not execute the
benchmark. The exact Cargo and Rustc verbose bytes are equivalence receipts
consistent with the captured toolchain. The canonical profile-identity JSON is
an evidence-derived normalization of anchored profile semantics, not an
original historical file. Toolchain, command, and release-profile claims MUST
remain derived from and consistent with independently anchored evidence; a
self-consistent structured claim is insufficient.
Because the recovered command does not disable default features, the recorded
effective feature set is exactly the server package default `redb` feature.

`BuildIdentity.environment` means only normalized environment assignments
explicitly prefixed to the recorded command. It is `{}` for the literal
baseline command and contains only the candidate command's exact
`CARGO_TARGET_DIR` override. It does not claim to reconstruct either build's
full inherited process environment.
The candidate build-source bytes are deterministically recoverable from commit
`a17d501` and its historical inputs; this derivation does not rewrite the
legacy logical source identity, which remains the recorded historical HEAD plus
dirty diff. The canonical 45-entry bundle covers root `Cargo.toml` and
`rust-toolchain.toml`, `server/`, `core/`, plus the `bench` and `client` member
manifests; its independently reviewed SHA-256 is
`fbbb585bfb4404c9abfb01e807451045becf669361f9275c7450b21e88e87c13`.
The baseline complete source bundle is not yet recovered.

Bind the current capture to the candidate bundle through an explicit product
projection covering build-relevant `server/` and `core/` source plus root,
server, and core Cargo manifests. Driver-only benchmark or lockfile changes may
not invalidate that equality, but any product-projection difference MUST block.
The current Cargo lock is not asserted equal to the historical lock: its only
known transition is separately reviewed driver-only dependency work.
Hash-bind tracked changes and every untracked file's path, mode, length, and
content digest in the repository capture; do not claim that the capture retains
untracked file contents.

Bound all provenance inputs before allocation: any one input file is at most
64 MiB, a source bundle contains at most 4,096 entries, one source entry is at
most 8 MiB, and aggregate decoded source contents are at most 32 MiB. Perform
the complete sealed-plan and live-repository verification once before the
fixed execution series. Immediately before each spawn, retain the existing
descriptor/inode/hash verification of the selected executable only; do not
rehash both source bundles or the whole repository between measured children,
because that unbounded orchestration would perturb cache and thermal state.

Do not run or rebuild either product arm while implementing this remediation.
The final-source acceptance task remains blocked after the v3 migration until
the complete baseline source bundle is independently anchored and the exact
frozen C4 candidate executable is recovered, the request is successfully
composed and prepared, and the result is independently reviewed.

## Acceptance

- Request and plan schema v3 contain `ArtifactProvenance`, distinct
  `ArtifactOrigin` and `ProvenanceStatus`, `HistoricalSourceIdentity`,
  `DependencyIdentity`, `ToolchainIdentity`, `BuildIdentity`, canonical
  `FileInput`/`ArtifactInput`, a fixed independent legacy-evidence policy, a
  canonical complete `SourceBundle`, a mechanically derived source-tree
  manifest, and a mechanically derived reviewed patch with per-path operation
  and before/after mode, length, and hashes; the opaque v1 build/source identity
  fields no longer exist. Every acceptance-critical serialized struct rejects
  unknown fields and every tagged enum rejects noncanonical variants. Each arm
  carries rehashable role-specific legacy evidence, source
  snapshot/bundle/benchmark, lock/workspace/package/config, verbose toolchain,
  build-environment, and profile-identity input files corresponding to every
  structured claim. It does not invent a candidate source-status payload that
  was not recovered.
- `BuildIdentity.command_argv` is mechanically derived from independently
  anchored command evidence for each arm. Its environment map contains only
  explicit command-prefix assignments: empty for baseline and the exact
  `CARGO_TARGET_DIR` assignment for candidate. It never claims inherited process
  environment state or accepts additional caller-authored variables.
- Composition rejects a noncanonical, falsely original, or unanchored receipt,
  a rebuilt or wrong-role arm, either artifact hash outside the preregistered pair, any
  provenance/source/dependency/toolchain/build/patch mismatch, a malformed
  patch path or operation/hash tuple, and any symlink, alias, overlapping root,
  existing output, output nested within an input/repository, or changing input
  before creating a read-only request. It snapshots every input identity before
  validation and rehashes the whole set immediately before immutable publication.
- Composition and validation reject the stated file, source-entry, source-count,
  and aggregate source-bundle limits before unbounded allocation. Execution
  performs one complete plan/repository verification before the series and the
  selected executable's descriptor/inode/hash check immediately around every
  spawn; it does not perform full source-tree I/O between timed children.
- Composition parses each complete source bundle, derives and checks its
  manifest, mechanically derives the reviewed patch's complete ordered path
  transitions, and checks the current-source product projection. Preparation
  copies and re-verifies the executable, evidence-derived provenance receipt,
  exact legacy payloads, source bundle, mechanically derived patch manifest,
  canonical composed request bytes, and every bound
  source/dependency/toolchain/build input into the sealed plan. Composition,
  preparation, execution, and independent verification use one shared
  provenance validator. Execution and verification fail closed if any v3
  field, copied byte, file identity, plan seal, or policy binding drifts.
- Deterministic fixtures cover canonical v3 composition-to-preparation and
  preparation-to-execution/verification round trips, every
  rejection class above, immutable create-once output, and zero product
  invocations. Focused tests, strict Clippy, the full locked/offline workspace
  suite, and `spec lint` pass before this task is completed.
