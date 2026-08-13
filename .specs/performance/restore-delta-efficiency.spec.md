---
id: TASK:performance/restore-delta-efficiency
type: task
status: accepted
summary: >
  Apply the evidence-selected delta reuse fix and verify its byte savings
  without hiding correctness or full-transfer regressions.
owners: [carlo]
progress: done
refines:
  - REQ:performance/proportional-delta-sync#c-target
  - REQ:performance/proportional-delta-sync#c-honesty
aspects: [delta, performance]
assignee: carlo
eta:
blocked_by: [TASK:performance/diagnose-delta-reuse]
---

# Restore delta efficiency

## Plan

Implement only the causal branch established by
`TASK:performance/diagnose-delta-reuse`:

- if reusable chunks disappear because client state is not recovered for the
  existing destination, repair persistence and destination mapping so the
  recorded v1 manifest supplies the update possession basis;
- if the complete local basis is present but FastCDC boundaries provide
  ineffective reuse for the measured edit, change the selected chunking policy
  or profile with a versioned, deterministic manifest contract and regression
  coverage for both revisions;
- if diagnosis is unclassified, do not change either subsystem and report this
  task blocked with the missing evidence.

Keep the diagnostic metrics from the first task. Re-run the same identified
corpus, optimized build profile, warm cache policy, loopback QUIC path, and five
paired full/update protocol before and after the fix. Do not substitute payload
bytes for QUIC wire bytes or compare results from unlike transports.

## Acceptance

- All five post-change full transfers and all five updates produce the exact
  expected complete artifact hash.
- Every post-change update reports no more than 4 MiB of QUIC wire bytes.
- The post-change median full-transfer wall time is no more than 5% slower than
  the five-pair pre-change median under the same measurement conditions.
- Raw before/after records show local basis, manifest, missing and received
  chunk counts, payload/wire bytes, client network/store timing, wall time, CPU,
  and RSS for every operation.
- If any correctness, update-byte, full-transfer-regression, or comparability
  criterion fails, the result is reported as a failed target and the task is not
  represented as complete.
