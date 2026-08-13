---
id: TASK:security/quic-admission-deadlines
type: task
status: accepted
summary: >
  Bound pre-authentication QUIC allocation and apply leak-free admission and
  progress deadlines to every server stream.
owners: [carlo]
progress: blocked
refines:
  - REQ:security/bounded-quic-service#c-preauth
  - REQ:security/bounded-quic-service#c-admission
  - REQ:security/bounded-quic-service#c-accounting
  - REQ:security/bounded-quic-service#c-deadlines
  - REQ:security/bounded-quic-service#c-release
  - REQ:security/bounded-quic-service#c-config
aspects: [preauth, admission, accounting, deadlines, release, configuration]
assignee: carlo
eta:
blocked_by: [TASK:performance/run-final-source-quic-fetch-acceptance]
---

# Quic admission deadlines

## Plan

Introduce typed, validated QUIC resource settings and shared admission guards
in the [server stream accept loop](spec:src:server/src/quic.rs), parse them
before secrets and storage in the [server launcher](spec:src:server/src/main.rs),
and validate a Register frame length prefix against the fixed 64 KiB ceiling through a
bounded variant of the [framed reader](spec:src:core/src/utils/frame.rs) before
allocation. Use fail-fast owned global and per-connection count permits for the
exact pending and active ceilings in the parent requirement; do not queue while
holding a QUIC stream. Because every reservation is fixed and startup validates
`count * reservation <= byte budget` without overflow, each global count permit
jointly owns one corresponding byte reservation and therefore enforces both
ceilings. Runtime current and peak byte counters are derived exactly from the
owned count reservations; a second byte semaphore would be mathematically
redundant and is not required. A handshake temporarily consumes global pending
capacity, a new stream acquires global and per-connection pending capacity before
its task is spawned, and Fetch/Sync transitions to active capacity immediately
after UUID authentication and before reading the request length prefix, request
payload, or performing storage work. A decoded Register obtains active
work/response capacity before successful enrollment can serialize metadata.

Wrap handshake, connection-idle `accept_bi`, prelude, UUID,
request/registration, possession, and each response write in phase deadlines,
and apply a fixed absolute deadline from stream admission across all phases. On
timeout or rejection, explicitly stop receive and reset send with stable
internal application codes; public protobuf messages and operation bytes remain
unchanged. Owned guards update current and peak reservation counters and must
release on success, failure, task abort, or connection close. Check the
absolute deadline between synchronous batch-building steps, without claiming
to preempt a filesystem read or compression already executing. IP policy,
wire-version negotiation, structured overload wire messages, and transfer
algorithm tuning are non-goals.

## Configuration surface

The launcher validates this environment contract before reading the enrollment
secret, state directory, metadata, or storage configuration:

| Setting | Default | Hard limit |
| --- | ---: | ---: |
| `DISTD_QUIC_MAX_PENDING_STREAMS` | 64 | 64 |
| `DISTD_QUIC_MAX_PENDING_STREAMS_PER_CONNECTION` | 4 | 4 |
| `DISTD_QUIC_PREAUTH_BUDGET_MIB` | 4 MiB | 4 MiB |
| `DISTD_QUIC_MAX_ACTIVE_TRANSFERS` | 4 | 4 |
| `DISTD_QUIC_MAX_ACTIVE_TRANSFERS_PER_CONNECTION` | 2 | 2 |
| `DISTD_QUIC_ACTIVE_BUDGET_MIB` | 256 MiB | 256 MiB |
| `DISTD_QUIC_HANDSHAKE_TIMEOUT_MS` | 10,000 ms | 86,400,000 ms |
| `DISTD_QUIC_CONNECTION_IDLE_TIMEOUT_MS` | 60,000 ms | 86,400,000 ms |
| `DISTD_QUIC_PRELUDE_TIMEOUT_MS` | 5,000 ms | 86,400,000 ms |
| `DISTD_QUIC_UUID_TIMEOUT_MS` | 5,000 ms | 86,400,000 ms |
| `DISTD_QUIC_REQUEST_TIMEOUT_MS` | 30,000 ms | 86,400,000 ms |
| `DISTD_QUIC_WRITE_TIMEOUT_MS` | 30,000 ms | 86,400,000 ms |
| `DISTD_QUIC_STREAM_LIFETIME_MS` | 900,000 ms | 86,400,000 ms |

The request deadline also bounds the possession phase. Byte budgets are fixed
reservations rather than measured allocator usage: 64 KiB for every pending
stream and 64 MiB for every active transfer.

Quinn exposes bidirectional streams lazily: an `open_bi` followed by an empty
write does not emit a STREAM frame, while the first non-empty byte necessarily
completes or invalidates the one-byte operation prelude. The production Prelude
deadline therefore uses the same generic bounded-read wrapper tested with a
pending reader; a network-level zero-byte unfinished Prelude test would require
a lower-level raw QUIC frame harness rather than Quinn's public stream API.

## Acceptance

- A complete Register frame at 64 KiB reaches decoding, while a prefix
  declaring 64 KiB plus one or 32 MiB is rejected before a payload buffer is
  allocated. Thirty-two concurrent declarations never raise pending
  count above 64 globally/four per connection or reserved pre-auth bytes above
  4 MiB, and all locally admitted handlers release by deadline.
- Multi-connection tests saturate the 64/four pending and four/two active
  limits. Excess work is immediately stopped/reset before storage access, and
  peak counters satisfy both checked byte equations. Counter values return to
  zero after success, invalid token/UUID, parse failure, timeout, peer close,
  and explicit task cancellation.
- With all active permits held, a stream presenting a valid UUID followed by a
  Fetch or Sync request prefix declaring 32 MiB is reset immediately after UUID
  authentication. Instrumentation proves the post-auth length prefix was not
  consumed, no declared-payload allocation occurred, and storage was not
  accessed.
- Deterministic tests stall handshake, `accept_bi`, prelude, UUID,
  registration/request, manifest/possession, and response writes. A trickle
  that remains inside each progress deadline still reaches the absolute stream
  lifetime; every case emits a credential-free phase/reason event.
- Startup tests cover zero, empty, non-UTF-8, overflow, per-connection greater
  than global, counts or budgets above their hard ceiling, unsatisfied checked
  equations, zero/over-24-hour deadlines, and an absolute lifetime shorter
  than a phase deadline.
- Existing Register, Fetch, and Sync messages remain byte-compatible and their
  integration tests pass on IPv4 and, when the host supports it, IPv6 loopback.
- Before implementation, preregister a release-mode microbenchmark that uses
  the real server handler, one already enrolled client/connection per arm, and
  sequential authenticated Fetch streams. Check in deterministic server
  metadata and the exact expected length-delimited Fetch response byte length
  and BLAKE3 hash; every response in both arms MUST match them, making response
  correctness and wire identity prerequisites for a valid timing sample.
- Build each arm once in release mode and reuse that immutable artifact for all
  of its runs. Record exact host hardware, OS/power state, Rust toolchain,
  dependency lock, release flags, source revision/dirty digest, and both build
  identities. The acceptance driver MUST independently anchor recovered legacy
  evidence, derive each source manifest from complete rehashable source bytes,
  and mechanically derive the baseline-to-candidate patch; a caller-authored
  identity or patch is insufficient. Build command argv and explicit command
  environment overrides MUST likewise derive from independently anchored
  command evidence; they do not claim inherited process environment state.
  Baseline and candidate product source identities MUST differ only by the
  reviewed task patch. Under that frozen environment, run exactly one
  unmeasured 5,000-stream warm-up for each arm in preregistered B-then-C order,
  then exactly five measured chronological pairs in the fixed order
  `BC/CB/BC/CB/BC`, with one 5,000-stream batch for each arm in each pair.
- The primary metric is the ratio of candidate to baseline arm medians in
  nanoseconds per operation across their five measured batches and MUST be at
  most 1.05. Report every within-pair candidate-minus-baseline delta as
  supporting evidence, but do not substitute it for the primary gate. Preserve
  chronological per-stream and per-batch raw results without discarding or
  opportunistically rerunning samples. A preregistered environmental failure
  invalidates the complete five-pair experiment; retain its results and reason
  before running one complete replacement experiment. End-to-end tests provide
  correctness and unchanged-wire evidence only, not a performance acceptance
  metric.
- Focused frame, admission, deadline, and cancellation tests pass, followed by
  the full validation suite in the [README](spec:src:README.md).
