---
id: REQ:trust/publisher-authenticity
type: requirement
status: accepted
level: MUST
summary: >
  Clients accept an artifact only when its canonical manifest statement carries
  a valid signature from an explicitly trusted software publisher.
owners: [carlo]
refines: [REQ:product/artifact-delivery#c-authenticity]
categorized_under: []
---

# Publisher authenticity

## Context

Certificate pinning authenticates the distribution server, not the release
producer. Consequently a compromised server can currently replace both the
manifest and bytes while still satisfying BLAKE3 integrity checks. This cycle
closes that boundary without attempting rollback policy.

:::{requirement id="publisher-authenticity" level="MUST"}
- {#c-statement} distd MUST define one versioned, domain-separated, canonical
  byte statement covering artifact ID, root hash, total size, ordered chunk
  hashes and sizes, chunking profile, and ordered file entries including their
  paths, sizes, and chunk ranges.
- {#c-key-policy} Publisher trust MUST use an explicit allowlist of Ed25519
  public keys identified by stable fingerprints; TLS server keys, enrollment
  secrets, and publication bearer tokens MUST NOT implicitly grant publisher
  authority.
- {#c-publication} A publisher-enforcing server MUST reject a missing,
  malformed, untrusted, or invalid signature before advancing or persisting the
  artifact's visible version metadata.
- {#c-publication-locks} Publication serialization MUST retain one lock slot
  only while an active or waiting coordinator references its artifact ID.
  Success, rejection, failure, and cancellation MUST release the participant,
  and the last participant MUST remove the slot without weakening same-artifact
  serialization or different-artifact independence.
- {#c-propagation} The accepted publisher key identity and detached signature
  MUST be persisted with the immutable revision and returned with its sync
  manifest without server-side substitution.
- {#c-client} A publisher-enforcing client MUST validate the complete canonical
  statement and signature before requesting artifact chunks or mutating
  destination state, and MUST fail closed on unsigned or unknown formats.
:::

## Compatibility and non-goals

Secure mode does not silently trust unsigned v0.2 revisions. Any research-only
unsigned compatibility mode must be explicit and visibly insecure. This first
cycle does not manage private signing keys, build a signing CLI or PKI, rotate
or revoke keys, prevent replay of an already valid revision, choose rollbacks,
or add transparency logging. External release tooling is responsible for
producing signatures with the shared canonical statement contract.
