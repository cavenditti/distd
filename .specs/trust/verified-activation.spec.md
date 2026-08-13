---
id: INV:trust/verified-activation
type: invariant
status: accepted
summary: >
  No artifact bytes become active before provenance, content, structure, and
  destination checks all succeed.
owners: [carlo]
enforcement:
  - REQ:product/artifact-delivery#c-integrity
  - REQ:product/artifact-delivery#c-destination
  - REQ:product/artifact-delivery#c-activation
  - REQ:trust/publisher-authenticity#c-client
applies_to:
  - REQ:product/artifact-delivery
  - REQ:trust/publisher-authenticity
---

# Verified activation

:::{invariant id="verified-activation"}
For every client activation, the publisher signature, canonical manifest,
ordered chunk hashes and sizes, complete root and size, file-entry structure,
and destination containment MUST already be valid. Any failure MUST preserve
the prior destination and MUST NOT register the candidate revision as active
local state.
:::

The existing [staging and atomic replacement implementation](spec:src:core/src/chunk_storage/fs_storage.rs)
is the activation boundary that publisher verification must precede.
