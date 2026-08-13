---
id: INV:trust/separate-trust-roots
type: invariant
status: accepted
summary: >
  Distribution access and transport identity never imply authority to publish
  software accepted by clients.
owners: [carlo]
enforcement:
  - REQ:trust/publisher-authenticity#c-key-policy
  - REQ:trust/publisher-authenticity#c-client
applies_to:
  - REQ:product/artifact-delivery
  - REQ:trust/publisher-authenticity
---

# Separate trust roots

:::{invariant id="separate-trust-roots"}
Possession of a valid server TLS key, client enrollment secret, publication
bearer token, server metadata store, or artifact chunk store MUST NOT be
sufficient to produce a newly publisher-authenticated artifact. Publisher
authority exists only when the canonical statement verifies under an explicitly
trusted publisher key.
:::
