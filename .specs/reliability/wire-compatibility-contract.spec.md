---
id: TASK:reliability/wire-compatibility-contract
type: task
status: accepted
summary: >
  Define and fixture-test the independent version declaration and evolution
  rules for the public QUIC operation and protobuf wire contract.
owners: [carlo]
progress: done
refines:
  - REQ:reliability/wire-compatibility#c-version
  - REQ:reliability/wire-compatibility#c-evolution
aspects: [version, evolution]
assignee: carlo
eta:
blocked_by: []
---

# Wire compatibility contract

## Plan

Add a fixed 12-byte declaration before `TransportOp` in the [operation
layer](spec:src:core/src/transport.rs). The exact V1 request preface is the ASCII
domain `DSWP`, followed by big-endian `u16` values for preface format `1`,
operation layer `1`, protobuf contract `1`, and reserved `0`. The declaration
is sent on every bidirectional operation stream. The server validates all 12
bytes before reading the operation byte, UUID, length prefix, or protobuf body.

A supported `(operation=1, protobuf=1)` pair continues directly with the
existing operation byte and therefore leaves every successful Register, Fetch,
and Sync response byte-identical. A complete malformed or unsupported preface
receives one fixed 12-byte response and FIN before the receive side is stopped:
ASCII domain `DSWR`, response format `1`, a big-endian status, and the server's
supported operation/protobuf versions. Status `0` is reserved; `1` means an
unsupported preface format, `2` an unsupported operation version, `3` an
unsupported protobuf version, `4` an unsupported pair, and `5` a malformed
domain/reserved field. Other statuses are reserved. Truncated input completed
with FIN receives status `5`; an unfinished preface remains governed by the
existing Prelude and absolute stream deadlines. Rejection never reads or
drains operation-specific bytes. The client recognizes `DSWR` in place of the
first response frame length and reports a typed compatibility error.

V1 operation identifiers are `1=Register`, `2=Fetch`, and `3=Sync`; `0` and
`4..=255` are reserved and rejected. V1 protobuf field numbers and enum values
are never reused. A removed identifier must be declared `reserved` by number
and name in the schema. Enum zero remains an unspecified/error sentinel.
Additive fields are V1-compatible only when an older reader may safely ignore
their complete semantics; a new required meaning, changed scalar encoding,
changed oneof meaning, or reinterpretation of an existing value requires a
protobuf-contract version bump. Unknown enum/profile values continue to fail at
the typed semantic boundary. The compatibility matrix is deliberately one row:
V1 preface/operation/protobuf reads and writes V1 in both directions; every
other version or mixed pair is rejected rather than negotiated or downgraded.

Preserve the resource-admission and absolute-deadline guarantees established by
the prerequisite. Compatibility checks use only fixed stack buffers and do not
read or rewrite server, storage, activation, or client trust state. This task
MUST NOT migrate any durable-state format.

## Acceptance

- Golden byte fixtures cover every supported preface and protobuf version,
  compatibility direction, and reserved identifier rule.
- Supported peers complete Register, Fetch, and Sync; unsupported or malformed
  versions receive a deterministic bounded rejection before operation payload
  decoding and release admission capacity.
- The supported path adds no compatibility round trip and preserves the exact
  pre-existing successful response protobuf bytes; only the 12-byte request
  preface is new.
- A compatibility test proves wire changes neither read nor rewrite durable
  state solely to determine protocol compatibility.
- Focused transport and integration tests pass, followed by the full documented
  validation suite.
