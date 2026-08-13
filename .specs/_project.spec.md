---
id: PROJECT:distd
type: project
status: accepted
summary: >
  Authenticated, bandwidth-efficient delivery of immutable artifact revisions
  from one origin to explicitly configured clients over QUIC.
owners: [carlo]
---

# Distd

## Purpose

Give release operators a verifiable way to publish large files and directory
artifacts while allowing managed clients to reuse chunks they already possess
and replace a destination only after the complete revision is valid.

## Scope

distd owns artifact identity and immutable revisions, deterministic manifests
and chunking, authenticated publication and enrollment, QUIC synchronization,
content verification, local staging, and atomic destination replacement. The
current product is deliberately a single-origin synchronizer; its implemented
v0.2 boundary is summarized in the [README](spec:src:README.md).

## Non-goals

The current project does not provide peer discovery, swarms, multi-source
scheduling, package dependency resolution, release-channel policy, automatic
rollback selection, transparency logs, or protection from a hostile local
process racing filesystem operations. Publisher authenticity is the next
bounded trust-boundary cycle; rollback policy remains separate work.

## Principles

Artifact identity is independent of transport and destination paths. Revisions
are immutable. Remote metadata never chooses a client destination. Transport
authentication, publisher authenticity, and client enrollment are distinct
trust decisions. Bytes become active only after all applicable authenticity,
integrity, containment, and completeness checks pass. Performance claims must
come from correctness-checked workloads and comparable transport conditions.
