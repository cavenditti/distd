---
id: TASK:reliability/wire-compatibility-contract
type: task
status: accepted
summary: >
  Define and fixture-test the independent version declaration and evolution
  rules for the public QUIC operation and protobuf wire contract.
owners: [carlo]
progress: pending
refines:
  - REQ:reliability/wire-compatibility#c-version
  - REQ:reliability/wire-compatibility#c-evolution
aspects: [version, evolution]
assignee: carlo
eta:
blocked_by: [TASK:security/quic-admission-deadlines]
---

# Wire compatibility contract

## Plan

Add a checked version declaration before `TransportOp` in the [operation
layer](spec:src:core/src/transport.rs), publish the compatibility matrix and
protobuf field/tag/enum evolution rules, and reject unsupported peers through a
small bounded response before operation payload decoding. Preserve the resource
admission and absolute-deadline guarantees established by the prerequisite.
This task MUST NOT migrate server, storage, activation, or client trust files.

## Acceptance

- Golden byte fixtures cover every supported preface and protobuf version,
  compatibility direction, and reserved identifier rule.
- Supported peers complete Register, Fetch, and Sync; unsupported or malformed
  versions receive a deterministic bounded rejection before operation payload
  decoding and release admission capacity.
- A compatibility test proves wire changes neither read nor rewrite durable
  state solely to determine protocol compatibility.
- Focused transport and integration tests pass, followed by the full documented
  validation suite.
