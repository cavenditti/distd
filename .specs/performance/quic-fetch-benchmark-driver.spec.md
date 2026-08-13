---
id: TASK:performance/quic-fetch-benchmark-driver
type: task
status: accepted
summary: >
  Replace temporary shell evidence roots with one repository-owned driver for
  the frozen QUIC authenticated-Fetch acceptance protocol.
owners: [carlo]
progress: done
refines:
  - REQ:performance/reproducible-acceptance-benchmarks#c-plan
  - REQ:performance/reproducible-acceptance-benchmarks#c-preflight
  - REQ:performance/reproducible-acceptance-benchmarks#c-execution
  - REQ:performance/reproducible-acceptance-benchmarks#c-evidence
  - REQ:performance/reproducible-acceptance-benchmarks#c-portability
aspects: [benchmark, reproducibility, evidence]
assignee: carlo
eta:
blocked_by: []
---

# Quic Fetch benchmark driver

## Plan

Add a small driver owned by the benchmark workspace with separate `capture-source`, `prepare`, and `execute` operations. `capture-source` is non-product: it records canonical HEAD, tracked/staged/untracked bytes, status, lockfile, QUIC source, and accepted task identity in a read-only bundle outside the repository. `prepare` canonicalizes paths, verifies the frozen baseline and candidate executables and source identities, validates the exact authenticated-Fetch plan, probes loopback capability in the intended execution context, copies every execution input into a driver-owned read-only plan tree, and seals that plan without invoking either product arm. Preflight environment or capability failures produce `BLOCKED` and zero product invocations.

`execute` is the only product-authorized entry. It consumes one sealed plan, creates one absent evidence root, and owns the complete state machine: warm-up baseline, warm-up candidate, then `BC/CB/BC/CB/BC`, with 5,000 sequential Fetch streams per invocation. It retains ordered events, child output and status, raw samples, response identity and FIN evidence, and an immutable first terminal result. The audit accepts only the exact twelve assignments and computes five arm medians, their primary ratio, and five chronological paired deltas. Do not rebuild the already frozen baseline or final-source candidate.

Keep the driver out of the measured child process. Adding it MUST NOT change the frozen product binaries or the authenticated Fetch workload.

## Acceptance

- `capture-source` emits a read-only, non-product repository identity bundle outside the repository, binding canonical HEAD, tracked/staged/untracked bytes, status, lockfile, QUIC source, and accepted task bytes.
- `prepare` accepts only canonical, existing artifacts whose hashes, source, lockfile, fixture, response length/hash, run order, sample count, and threshold match the declared plan; it copies every input into a driver-owned 0555 plan tree and its sealed output is independent of caller cwd.
- A failed source, artifact, environment, or IPv4-loopback capability check returns `BLOCKED`, retains its reason, and proves zero product invocations.
- One successful execution invokes exactly twelve children in the sealed order, retains 60,000 positive chronological samples, validates every 217-byte response against BLAKE3 `9b66dc2d89048d154be69e8e43be058df685224c58234a4f1fd9b86320122bb5`, and requires FIN before the next stream.
- The terminal audit rejects missing, duplicate, extra, misassigned, malformed, partial, hash-drifted, or signalled runs before computing five baseline and five candidate medians. It reports the primary candidate/baseline median ratio and all five candidate-minus-baseline paired deltas.
- Deterministic fixtures cover preflight blocking, pass, threshold failure, child nonzero/partial output, signal propagation, hash drift, duplicate/extra invocation attempts, first-failure immutability, and evidence re-verification.
- The local driver trust boundary excludes a hostile process running as the same operating-system user. Because macOS exposes no descriptor-based executable primitive, execution is by canonical pathname from a driver-owned 0555 plan tree; the driver hashes and retains the inode immediately before spawn, rechecks the same path/inode after spawn, and fails closed on drift. Plan and evidence operations remain descriptor-pinned against other-user path swaps.
- Focused driver tests pass, followed by the full validation suite documented in the [README](spec:src:README.md).
