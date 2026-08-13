---
id: REQ:performance/reproducible-acceptance-benchmarks
type: requirement
status: accepted
level: MUST
summary: >
  Performance acceptance uses a repository-owned, fail-closed driver whose
  plans, product invocations, evidence, and terminal verdict are reproducible
  without cwd-sensitive shell orchestration.
owners: [carlo]
refines: []
categorized_under: []
---

# Reproducible acceptance benchmarks

## Context

The final-source QUIC Fetch acceptance attempt created thirteen successive
temporary evidence roots. Ten were lost to harness or orchestration defects,
three stopped on environmental gates, one baseline warm-up executable was
started, and zero timed batches completed. The benchmark implementation did not
produce a measured failure; the evidence machinery prevented a valid result.
Acceptance infrastructure therefore needs the same deterministic state,
identity, and failure semantics expected from product code.

:::{requirement id="reproducible-acceptance-benchmarks" level="MUST"}
- {#c-plan} Before any product invocation, a repository-owned driver MUST seal
  one immutable plan containing canonical absolute artifact paths and hashes,
  source and dependency identities, exact run order and counts, correctness
  identity, metric definition, threshold, and one absent output root.
- {#c-preflight} Source, artifact, environment, and transport-capability checks
  MUST finish before the experiment begins. A failed preflight MUST return a
  distinct blocked outcome with zero product invocations; it MUST NOT consume,
  partially create, or require replacing an experiment.
- {#c-execution} After the first warm-up starts, the driver MUST enforce the
  sealed order internally, permit no retry or extra invocation, propagate child
  and signal status, and preserve the first terminal failure immutably.
- {#c-evidence} Every child stdout, stderr, assignment, artifact hash, raw
  sample, correctness result, and exit status MUST be retained under one unique
  append-only output root. The audit MUST bind records to the exact plan before
  computing the primary metric and every required supporting metric.
- {#c-portability} The driver MUST be independent of caller working directory,
  shell executable bits, relative checksum bases, and externally sequenced
  runner/auditor calls. Its state machine and evidence schema MUST be covered by
  deterministic fixtures for blocked, passing, threshold-failing, malformed,
  interrupted, drifted, duplicate, and partial executions.
:::

## Non-goals

This requirement does not change the benchmark workload, relax an existing
performance threshold, select favorable environment observations, rebuild the
frozen product arms, or make benchmark tooling part of a public wire contract.
