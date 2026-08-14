---
id: REQ:trust/rollback-protection
type: requirement
status: accepted
level: MUST
summary: >
  Prevent a distribution server from replaying an older valid publisher release
  after a managed client has authenticated a newer release of the same artifact.
owners: [carlo]
refines: [REQ:trust/publisher-authenticity]
categorized_under: []
---

# Rollback protection

## Context

Publisher signatures currently authenticate artifact content but deliberately
exclude the server-assigned revision. A compromised distribution server can
therefore replay any older, correctly signed manifest and bytes. Comparing the
unsigned server revision is insufficient because the same server can relabel an
old signed statement with a larger revision. Rollback protection needs a
publisher-authoritative monotonic value and a durable client observation.

:::{requirement id="rollback-protection" level="MUST"}
- {#c-sequence} Every rollback-protected release MUST carry an artifact-scoped
  unsigned-64-bit sequence inside the canonical publisher-signed statement;
  server-assigned revision, transport order, and timestamps MUST NOT substitute
  for that publisher authority.
- {#c-decision} For one canonical server authority and artifact ID, a managed
  client MUST reject a sequence lower than its durable high-water mark, accept
  the same sequence only with the exact previously accepted statement identity,
  and MAY accept a higher sequence signed by any key currently authorized by
  that server's explicit publisher policy.
- {#c-durability} The client MUST durably bind the highest accepted sequence,
  exact statement identity, artifact ID, and canonical server authority before
  an authenticated candidate can replace the active destination. Interruption
  MUST recover to a state that cannot accept an older sequence; an exact
  same-sequence retry MUST remain possible.
- {#c-boundary} Rollback rejection MUST occur before possession or artifact
  chunk acceptance and MUST leave the prior active revision, destination,
  chunk-cache ownership, and activation state unchanged.
- {#c-compatibility} A rollback-enforcing client MUST fail closed on the legacy
  sequence-free V1 publisher statement. Any authenticity-only compatibility
  mode MUST be explicit, visibly replayable, and MUST NOT create a synthetic
  sequence or mark V1 history as rollback-protected.
:::

## Authority and limitations

The high-water mark is scoped to the configured canonical server authority and
artifact ID, but not to one publisher key, so adding or rotating an allowlisted
key cannot reset an already observed sequence. A trusted publisher may
intentionally authorize older content at a higher sequence; that is a new
release, not an undetectable server rollback. A fresh client with no durable
observation cannot prove that the first valid release it sees is globally
latest. Cross-client transparency, publisher key revocation, global release
logs, and recovery after deletion of the client's durable state remain separate
requirements.
