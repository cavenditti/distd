---
id: TASK:storage/bound-tree-node-cache
type: task
status: accepted
summary: >
  Replace duplicate tree-node recency records with exact constant-time LRU
  ownership under the existing 65,536-entry ceiling.
owners: [carlo]
progress: done
refines:
  - REQ:storage/durable-artifact-state#c-tree-node-cache
assignee: carlo
eta:
blocked_by: []
aspects: [storage, cache, memory, lru, evidence]
---

# Bound tree-node cache recency

## Plan

Store older/newer hashes beside each cached node with explicit oldest and
newest endpoints. Hits detach one live entry and append it as most-recently
used in constant time. Replacement removes the prior cache ownership before
installing one new entry. Pressure always removes the oldest cache entry even
when callers retain independent `Arc<Node>` references.

The linked order is an exact permutation of the live hash map: empty endpoints
iff the map is empty, unique bidirectional links, and one reachable node per
entry. A zero-entry configuration bypasses admission.

## Acceptance

- One million hits retain exactly one recency relation per live node and make
  the touched node most-recently used.
- With an external owner of the oldest node, admitting one node at capacity
  removes the cache's oldest entry while the external node remains valid.
- A release reference gates one million hits at 100 milliseconds and 10,000
  same-hash replacements at 100 microseconds p95, ending with one entry and one
  recency node.
- Focused/full tests, strict Clippy, formatting, exact-baseline spec lint, and
  requirement coverage pass.
