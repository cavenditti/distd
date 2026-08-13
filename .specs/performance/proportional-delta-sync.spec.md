---
id: REQ:performance/proportional-delta-sync
type: requirement
status: accepted
level: MUST
summary: >
  Correct delta updates transfer bytes proportional to missing content without
  materially regressing equivalent full transfers.
owners: [carlo]
refines: [REQ:product/artifact-delivery#c-efficiency]
categorized_under: []
---

# Proportional delta sync

## Context

An isolated optimized-build baseline on 2026-08-12 used a 32 MiB random
single-file artifact whose second revision changed seven of 128 aligned 256 KiB
regions (nominally 5%). The full fetch sent 34,320,734 QUIC bytes and the update
sent 34,320,165 bytes while both output hashes were correct. Raw evidence was
retained at
`/private/tmp/distd-v02-baseline.2kfiuP/bounded-results/results.json` for that
run, but this requirement records the relevant observation and does not depend
on that ephemeral path. The benchmark must regenerate and identify its corpus
for every repeatable measurement.

:::{requirement id="proportional-delta-sync" level="MUST"}
- {#c-diagnosis} A delta benchmark MUST expose enough per-operation evidence to
  distinguish loss of the client's reusable local basis from ineffective
  manifest/chunk reuse: local basis chunk count, manifest chunk count, missing
  chunk count, payload bytes, QUIC wire bytes, and client network/store timing.
- {#c-correctness} Every measured full transfer and update MUST validate the
  destination against the expected complete artifact hash before its metrics
  are eligible for comparison.
- {#c-target} Across five paired warm runs of the pinned 32 MiB, 5%-edit
  workload, update QUIC wire bytes MUST be at most 4 MiB in every run and the
  median full-transfer wall time MUST regress by no more than 5% from the
  pre-change median under the same build, machine, cache, and transport
  conditions.
- {#c-honesty} Reports MUST retain raw per-run evidence, state the exact build
  and workload identities, compare only like-for-like paths, and report failure
  rather than claiming delta efficiency when correctness, byte, timing, or
  comparability criteria are unmet.
:::

## Non-goals

This requirement does not establish cross-tool speed rankings, cold-cache
performance, WAN behavior, resume support, or a general memory target.
