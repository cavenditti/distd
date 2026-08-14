---
id: TASK:reliability/bound-publication-structure
type: task
status: accepted
summary: >
  Bound retained multipart file metadata before field payload extraction and
  reuse the canonical publisher statement limits.
owners: [carlo]
progress: done
refines:
  - REQ:reliability/bounded-publication-ingress#c-structure
assignee: carlo
eta:
blocked_by: []
aspects: [publication, multipart, memory, canonicalization, evidence]
---

# Bound publication structure

## Plan

Expose the canonical publisher limits for entry count, entry-path bytes, and
statement bytes from the core signature module. During authenticated multipart
extraction, validate the next retained `item` field against those exact limits
before calling its payload reader. Keep accepted paths as keys in one map so
duplicate detection and returned ownership do not retain two `PathBuf` copies.
Move keys and payloads into the existing publication vector without deep
cloning; the server's canonical UTF-8 byte sort remains authoritative before
statement construction.

Only retained `item` fields count. Unrecognized multipart fields remain
unretained and bounded by the existing declared-body reservation and absolute
extraction deadline. Count excess and aggregate path-byte excess are `413`;
an overlong, duplicate, or noncanonical path is `400`. Rejection happens before
the rejected field body is read and therefore before storage preparation.

## Acceptance

- Pure boundary tests prove 65,536 item paths and 4,096 bytes per path are
  accepted, while count/path +1 is rejected with exact stable status. Checked
  aggregate path arithmetic accepts exactly 32 MiB and rejects +1 or overflow.
- Duplicate, absolute, traversal, and overlong paths fail without changing the
  retained file count or path-byte accounting. Missing filenames retain the
  existing canonical `item` fallback.
- A partial multipart request containing an overlong filename returns `400`
  after its headers while its body remains stalled, releases the aggregate
  admission, and performs zero storage or metadata mutation.
- A release reference validates and retains 65,536 prebuilt unique short paths
  with one owner each, preserves raw campaign samples, asserts exact final
  count/accounting, and gates p95 at 100 milliseconds.
- Focused/full tests, strict Clippy, formatting, spec lint, and coverage pass.
