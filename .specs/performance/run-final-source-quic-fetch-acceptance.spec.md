---
id: TASK:performance/run-final-source-quic-fetch-acceptance
type: task
status: accepted
summary: Restore the exact frozen QUIC Fetch arms and execute one sealed final-source acceptance experiment.
owners: [carlo]
progress: pending
refines:
  - REQ:performance/reproducible-acceptance-benchmarks#c-plan
  - REQ:performance/reproducible-acceptance-benchmarks#c-preflight
  - REQ:performance/reproducible-acceptance-benchmarks#c-execution
  - REQ:performance/reproducible-acceptance-benchmarks#c-evidence
assignee:
eta:
blocked_by: []
aspects: [plan, preflight, execution, evidence]
---

# Run final source quic fetch acceptance

## Plan

Recover the exact frozen baseline and final-source candidate executables plus their source-identity receipts without rebuilding or substituting either arm. Verify baseline SHA-256 `d60c56edf574a9980b3ba9e7572f9da95ca80260e530781495bdff4604979478` and candidate SHA-256 `6cb669536efea5b3055c6f4487624b96abc136cc5d036cbfaae672a651fcb327`. Build and freeze the repository-owned `quic-fetch-acceptance` driver, use its non-product `capture-source` and `prepare` operations to seal current source, artifacts, environment, the 217-byte response identity, the 5,000-stream batches, and the fixed `BC/CB/BC/CB/BC` order.

Stop for explicit product-execution authority after PREPARED. Then invoke `execute` exactly once and independently run `verify` over the resulting evidence root. Never reinterpret the C3 result as acceptance for the byte-different final-source C4 artifact, rebuild an arm without new authority, substitute an artifact, retry a run, or omit a BLOCKED/INVALID/FAIL result.

## Acceptance

- The baseline and candidate executables and their source-identity receipts are recovered byte-for-byte and match their sealed hashes; no arm is rebuilt or substituted.
- `capture-source` and `prepare` complete after the driver and specs are frozen, with zero product invocations and a distinct PREPARED or BLOCKED receipt.
- A PREPARED plan is executed only after explicit product authority and invokes exactly the sealed twelve children with no retry, extra, or exclusion.
- Independent `verify` authenticates the complete evidence root, all 60,000 samples, every 217-byte response/hash/FIN result, both arm medians, the exact primary ratio gate of at most 1.05, and all five chronological paired deltas.
- The final outcome is reported plainly as PASS, FAIL, INVALID, or BLOCKED and is never inferred from the earlier byte-different C3 experiment.
