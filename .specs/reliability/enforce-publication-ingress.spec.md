---
id: TASK:reliability/enforce-publication-ingress
type: task
status: accepted
summary: >
  Add fail-fast byte-weighted REST publication admission before multipart
  buffering and retain its permit through durable completion.
owners: [carlo]
progress: done
refines:
  - REQ:reliability/bounded-publication-ingress#c-config
  - REQ:reliability/bounded-publication-ingress#c-declaration
  - REQ:reliability/bounded-publication-ingress#c-admission
  - REQ:reliability/bounded-publication-ingress#c-lifetime
  - REQ:reliability/bounded-publication-ingress#c-observability
assignee: carlo
eta:
blocked_by: []
aspects: [configuration, admission, retention, cancellation, evidence]
---

# Enforce publication ingress

## Plan

Parse `DISTD_PUBLICATION_MEMORY_MIB` once at startup with a 512 MiB default and
strict 1..=2,048 MiB bounds, convert it to bytes with checked arithmetic, and
pass the immutable budget into the HTTP application. Keep the existing 512 MiB
individual-body limit unchanged.

Install a byte-weighted semaphore middleware inside bearer and publisher-header
authorization but outside multipart extraction. Require `Content-Length`,
reject a request that can never fit with 413, reject temporary aggregate
exhaustion with 429, and retain the owned permit across the complete handler
future so every return, cancellation, and unwind releases it automatically.

Add exact config boundaries, middleware-order, no-declaration, too-large,
temporary-exhaustion, cancellation/release, success/release, and no-mutation
tests. Measure release-mode reserve/drop overhead and require it to remain
negligible relative to multipart parsing while the full REST and workspace
suites, strict configured Clippy, formatting, and spec lint remain clean.

## Acceptance

- Configuration absent/1/512/2,048 MiB produces exact checked byte budgets;
  zero, 2,049, empty, signed, fractional, leading-zero, nonnumeric, and
  overflowing values fail before server construction.
- Missing or malformed length fails before multipart extraction. A declared
  body above the aggregate budget returns 413; a fitting request racing held
  capacity returns 429 without waiting, then succeeds or reaches ordinary
  validation after the prior permit releases.
- Invalid bearer and untrusted publisher requests retain their existing 401
  and 403 precedence and consume no admission capacity. Body-limit 413 behavior
  remains after valid authorization and admission.
- Permit accounting returns exactly to the configured maximum after success,
  handler error, and dropped in-flight handler; no failed admission changes
  storage, live metadata, or durable metadata bytes.
- Release reference measurements preserve raw samples and show reservation
  overhead below 10 microseconds p95. All focused and full workspace gates pass.
