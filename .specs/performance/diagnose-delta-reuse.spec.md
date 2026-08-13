---
id: TASK:performance/diagnose-delta-reuse
type: task
status: accepted
summary: >
  Make delta reuse observable and establish a five-pair causal baseline without
  assuming whether client state or chunking is responsible.
owners: [carlo]
progress: done
refines:
  - REQ:performance/proportional-delta-sync#c-diagnosis
  - REQ:performance/proportional-delta-sync#c-correctness
  - REQ:performance/proportional-delta-sync#c-honesty
aspects: [observability, benchmark]
assignee: carlo
eta:
blocked_by: []
---

# Diagnose delta reuse

## Plan

Extend the existing client and harness metrics without changing sync semantics.
For both full and update operations, preserve machine-readable local basis chunk
count, manifest chunk count, server missing chunk count, received chunk count,
payload bytes, QUIC wire bytes, client network/store timing, wall time, CPU, RSS,
and final correctness. In particular, retain update client stderr/timing rather
than logging it only for the full-transfer path in the [distd benchmark
runner](spec:src:bench/src/runners/distd.rs).

Generate one identified 32 MiB v1/v2 corpus with the benchmark's nominal 5%
edit, record complete artifact hashes and the actual mutation inventory, and
reuse those exact bytes for five paired warm full/update runs from one optimized
build. Preserve raw per-run output and environment/build identity outside the
repository. Use evidence from the [local diff basis](spec:src:client/src/client.rs),
[destination mapping](spec:src:core/src/chunk_storage/fs_storage.rs), and
[server missing-chunk selection](spec:src:server/src/server.rs) to classify the
cause before proposing a fix:

- zero or unexpectedly incomplete local basis indicates client state
  persistence or destination lookup/mapping;
- a complete local basis with nearly all manifest chunks missing indicates
  ineffective FastCDC reuse for this edit pattern;
- mixed or contradictory evidence remains unclassified and blocks an
  implementation choice until further instrumentation resolves it.

## Acceptance

- One structured record for every full and update operation contains all
  required counters and timings; no required update metric is recovered by
  manually scraping transient logs.
- Five paired pre-change runs use one recorded corpus and exact optimized build,
  and all ten destinations match their expected complete artifact hashes.
- The report gives raw values plus median full-transfer wall time, identifies
  the causal class from the counters above, and explicitly remains
  unclassified if the evidence does not support either branch.
- No synchronization, persistence, destination-selection, or chunking behavior
  is changed in this task.
