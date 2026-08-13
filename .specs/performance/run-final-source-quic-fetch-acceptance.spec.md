---
id: TASK:performance/run-final-source-quic-fetch-acceptance
type: task
status: accepted
summary: Restore the exact frozen QUIC Fetch arms and execute one sealed final-source acceptance experiment.
owners: [carlo]
progress: blocked
refines:
  - REQ:performance/reproducible-acceptance-benchmarks#c-plan
  - REQ:performance/reproducible-acceptance-benchmarks#c-preflight
  - REQ:performance/reproducible-acceptance-benchmarks#c-execution
  - REQ:performance/reproducible-acceptance-benchmarks#c-evidence
assignee:
eta:
blocked_by: [TASK:performance/remediate-quic-fetch-artifact-provenance]
aspects: [plan, preflight, execution, evidence]
---

# Run final source quic fetch acceptance

## Plan

Recover the exact frozen baseline and final-source candidate executables plus
their source evidence without rebuilding or substituting either arm. Verify
baseline SHA-256
`d60c56edf574a9980b3ba9e7572f9da95ca80260e530781495bdff4604979478`
and candidate SHA-256
`6cb669536efea5b3055c6f4487624b96abc136cc5d036cbfaae672a651fcb327`.
The exact candidate C4 executable remains missing, so this task is BLOCKED.
The recovered legacy receipt payloads are byte-exact evidence anchors, but
their original paths and inode identities are unavailable; the v3 structured
receipt MUST therefore identify itself as evidence-derived rather than
original. Their corrected read-only recovery bundle is sealed by manifest
SHA-256 `ab2d154eeef3a2f1423f2ba480d7201d159ddf82a77a6e34059e19d8a089bbb4`.
Recover complete rehashable source bundles for both arms. Candidate
source is deterministically recoverable from commit `a17d501` and its exact
historical diff/lock/QUIC inputs; its canonical 45-entry build-source bundle is
independently anchored by SHA-256
`fbbb585bfb4404c9abfb01e807451045becf669361f9275c7450b21e88e87c13`.
The baseline complete source bundle remains a
separate independent-anchor recovery gate. The recovered baseline captured-environment payload
supports toolchain, lock, host, Cargo-configuration, and release-profile facts,
while the recovered preregistration and build-result payloads independently
record and confirm the exact baseline command, successful optimized build,
exact artifact, and zero benchmark execution. The normalized command-environment
identity records explicit command-prefix overrides only: `{}` for baseline and
the candidate's exact `CARGO_TARGET_DIR`; it makes no claim about inherited
process environment variables. Build and freeze the repository-owned
`quic-fetch-acceptance` driver, use its non-product `capture-source`,
`compose-request`, and `prepare` operations to seal current source, artifacts,
environment, the 217-byte response identity, the 5,000-stream batches, and the
fixed `BC/CB/BC/CB/BC` order.

Stop for explicit product-execution authority after PREPARED. Then invoke `execute` exactly once and independently run `verify` over the resulting evidence root. Never reinterpret the C3 result as acceptance for the byte-different final-source C4 artifact, rebuild an arm without new authority, substitute an artifact, retry a run, or omit a BLOCKED/INVALID/FAIL result.

## Acceptance

- The baseline and candidate executables and all exact legacy receipt
  payloads are recovered byte-for-byte and match their sealed hashes; both
  complete source bundles are present and mechanically reproduce their
  manifests and reviewed patch; no arm is rebuilt or substituted.
- `capture-source`, `compose-request`, and `prepare` complete after the driver
  and specs are frozen, with zero product invocations and a distinct PREPARED or
  BLOCKED receipt. A caller-selected policy, unanchored receipt, absent source
  bundle, or missing C4 artifact can only produce BLOCKED/INVALID, never PREPARED.
- A PREPARED plan is executed only after explicit product authority and invokes exactly the sealed twelve children with no retry, extra, or exclusion.
- Independent `verify` authenticates the complete evidence root, all 60,000 samples, every 217-byte response/hash/FIN result, both arm medians, the exact primary ratio gate of at most 1.05, and all five chronological paired deltas.
- The final outcome is reported plainly as PASS, FAIL, INVALID, or BLOCKED and is never inferred from the earlier byte-different C3 experiment.
