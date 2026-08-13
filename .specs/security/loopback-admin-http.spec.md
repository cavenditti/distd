---
id: TASK:security/loopback-admin-http
type: task
status: accepted
summary: >
  Keep plaintext publication and administration local by default and reject
  accidental remote exposure without an explicit insecure override.
owners: [carlo]
progress: done
refines: [REQ:product/artifact-delivery#c-admin-exposure]
assignee: carlo
eta:
blocked_by: []
---

# Loopback admin http

## Plan

Replace the hard-coded all-interface HTTP listener in the [server
launcher](spec:src:server/src/main.rs) with a configurable bind policy. The
default MUST be loopback. A plaintext non-loopback bind MUST fail during startup
unless a separately named, explicit insecure-remote override is enabled; the
failure must identify the unsafe setting without logging tokens. Preserve the
separate QUIC listener and the existing route-level bearer checks in the [REST
router](spec:src:server/src/rest_api.rs). Adding HTTPS, a reverse proxy, mTLS,
firewall management, or changing QUIC reachability is outside this task.

## Acceptance

- With no bind configuration, the HTTP listener is reachable on loopback and
  does not listen on wildcard or non-loopback addresses.
- A configured loopback address starts; a plaintext wildcard or non-loopback
  address fails closed unless the explicit insecure-remote override is true.
- Invalid addresses and empty/ambiguous override values fail with actionable
  startup errors and never include enrollment, publication, or admin secrets.
- Focused bind-policy tests cover IPv4 and IPv6 loopback, wildcard, and a
  representative non-loopback address; existing REST authorization and QUIC
  integration tests still pass, followed by the full validation suite in the
  [README](spec:src:README.md).
