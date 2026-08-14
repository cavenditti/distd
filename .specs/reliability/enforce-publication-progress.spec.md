---
id: TASK:reliability/enforce-publication-progress
type: task
status: accepted
summary: >
  Bound authenticated multipart body transfer without cancelling publication
  after its durable coordinator owns the validated payload.
owners: [carlo]
progress: done
refines:
  - REQ:reliability/bounded-publication-ingress#c-progress
assignee: carlo
eta:
blocked_by: []
aspects: [configuration, progress, cancellation, retention, evidence]
---

# Enforce publication progress

## Plan

Parse `DISTD_PUBLICATION_BODY_TIMEOUT_SECS` once at startup with a 600-second
default and strict 1..=86,400-second bounds. Carry the duration into publication
admission, create an absolute deadline only after bearer and publisher-header
authorization have succeeded and the declared byte reservation is owned, and
pass that deadline to the multipart handler through a private request
extension.

Move field collection into one bounded future. A missed deadline returns 408,
drops the partial multipart state and accumulated fields, and releases the
admission permit. End the deadline at complete extraction; preparation,
signature verification, storage commit, and durable metadata publication keep
their existing owned coordinator and reservation lifetime.

## Acceptance

- Configuration absent/1/600/86,400 seconds produces exact durations; zero,
  86,401, empty, signed, fractional, leading-zero, nonnumeric, and overflowing
  values fail before server construction.
- A valid authenticated request whose declared body stream yields a partial
  multipart field and then stalls returns 408 within its configured deadline.
  All admission capacity is restored and storage, live metadata, and durable
  metadata remain byte-identical.
- Invalid bearer and publisher requests retain 401/403 precedence without
  starting a deadline or consuming admission capacity. Successful complete
  bodies retain existing publication and release behavior.
- Focused and full workspace tests, strict configured Clippy, formatting, spec
  lint, and coverage all pass.
