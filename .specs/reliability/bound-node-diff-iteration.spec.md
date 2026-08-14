---
id: TASK:reliability/bound-node-diff-iteration
type: task
status: accepted
summary: >
  Make hash-tree difference iteration lazy, iterative, and independent of
  attacker-controlled declared payload sizes.
owners: [carlo]
progress: done
refines:
  - REQ:performance/proportional-delta-sync#c-diff-iteration
assignee: carlo
eta:
blocked_by: []
aspects: [memory, recursion, delta-sync, hash-tree, evidence]
---

# Bound node-difference iteration

## Plan

Replace eager recursive tree reconstruction and byte-size-derived reservation
with an explicit post-order work stack. The iterator retains only traversal
frames and completed child roots needed to reconstruct the next parent. Skip
closure calculation uses the same iterative traversal so deep inputs cannot
consume the call stack.

## Acceptance

- A one-node tree declaring `u64::MAX` bytes starts with exactly one work frame
  and yields normally without size-derived allocation.
- A depth-4,096 adversarial tree computes closure and yields the exact post-order
  node count without recursive traversal; completed-child state remains bounded.
- Existing diff ordering, subtree skipping, and changed-byte tests remain exact.
- A release reference traverses 65,535 nodes within 100 milliseconds and
  records the exact node count and elapsed time.
- Focused/full tests, strict Clippy, formatting, exact-baseline spec lint, and
  requirement coverage pass.
