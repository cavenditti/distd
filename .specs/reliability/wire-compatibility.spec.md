---
id: REQ:reliability/wire-compatibility
type: requirement
status: accepted
level: MUST
summary: >
  QUIC peers declare a bounded, evolvable wire contract before operation data
  is interpreted, independently of durable-state representation.
owners: [carlo]
refines: []
categorized_under: []
---

# Wire compatibility

## Context

The [transport operation layer](spec:src:core/src/transport.rs) begins directly
with an operation byte, and the [protobuf schema](spec:src:core/proto/distd.proto)
has no session-level compatibility declaration. This contract is intentionally
separate from on-disk compatibility: a storage correction must not wait for a
new network handshake, and a wire upgrade must not rewrite local state.

:::{requirement id="wire-compatibility" level="MUST"}
- {#c-version} A connection MUST negotiate or declare a bounded wire-format
  version before interpreting operation-specific data, and unsupported versions
  MUST be rejected before operation payload decoding.
- {#c-evolution} The public operation preface and protobuf messages MUST have a
  documented compatibility matrix, reserved identifier rules, bounded decoding,
  and golden fixtures for every declared compatibility direction.
:::

## Non-goals

This requirement does not require indefinite backward compatibility, rolling
multi-node deployment orchestration, feature negotiation, or changes to any
durable-state envelope.
:::
