use std::fmt::Debug;
use std::future::{Future, IntoFuture};
use std::net::SocketAddr;
#[cfg(not(test))]
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use distd_core::chunk_storage::ChunkStorage;
use distd_core::proto::sync_message::Msg;
use distd_core::proto::{
    ClientKeepAlive, ClientRegister, ManifestRequest, PossessionBitfield, SyncMessage,
};
use distd_core::transport::{TransportOp, WireRejection, WireRejectionStatus};
use distd_core::utils::frame::{
    encode_length_delimited, read_length_delimited_async_with_limit, read_optional_uuid_async,
    read_transport_op_async, read_wire_preface_async, write_length_delimited_async,
    write_wire_rejection_async, FrameError, MAX_FRAME_PAYLOAD_BYTES,
};
use prost::Message;
use quinn::{Endpoint, RecvStream, SendStream};
use rcgen::generate_simple_self_signed;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{timeout, timeout_at, Instant};

use crate::server::build_one_batch;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::error::Server as ServerError;
use crate::Server;

pub const REGISTER_FRAME_MAX_BYTES: usize = 64 * 1024;
const PENDING_RESERVATION_BYTES: usize = REGISTER_FRAME_MAX_BYTES;
const ACTIVE_RESERVATION_BYTES: usize = 64 * 1024 * 1024;
const MAX_PENDING_GLOBAL: usize = 64;
const MAX_PENDING_PER_CONNECTION: usize = 4;
const MAX_PENDING_BUDGET_BYTES: usize = 4 * 1024 * 1024;
const MAX_ACTIVE_GLOBAL: usize = 4;
const MAX_ACTIVE_PER_CONNECTION: usize = 2;
const MAX_ACTIVE_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const MAX_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

const QUIC_STREAM_OVERLOADED_CODE: u32 = 0x10;
const QUIC_STREAM_TIMEOUT_CODE: u32 = 0x11;
const QUIC_STREAM_PROTOCOL_CODE: u32 = 0x12;
const QUIC_STREAM_INTERNAL_CODE: u32 = 0x13;

const PENDING_GLOBAL_ENV: &str = "DISTD_QUIC_MAX_PENDING_STREAMS";
const PENDING_PER_CONNECTION_ENV: &str = "DISTD_QUIC_MAX_PENDING_STREAMS_PER_CONNECTION";
const PENDING_BUDGET_MIB_ENV: &str = "DISTD_QUIC_PREAUTH_BUDGET_MIB";
const ACTIVE_GLOBAL_ENV: &str = "DISTD_QUIC_MAX_ACTIVE_TRANSFERS";
const ACTIVE_PER_CONNECTION_ENV: &str = "DISTD_QUIC_MAX_ACTIVE_TRANSFERS_PER_CONNECTION";
const ACTIVE_BUDGET_MIB_ENV: &str = "DISTD_QUIC_ACTIVE_BUDGET_MIB";
const HANDSHAKE_TIMEOUT_MS_ENV: &str = "DISTD_QUIC_HANDSHAKE_TIMEOUT_MS";
const CONNECTION_IDLE_TIMEOUT_MS_ENV: &str = "DISTD_QUIC_CONNECTION_IDLE_TIMEOUT_MS";
const PRELUDE_TIMEOUT_MS_ENV: &str = "DISTD_QUIC_PRELUDE_TIMEOUT_MS";
const UUID_TIMEOUT_MS_ENV: &str = "DISTD_QUIC_UUID_TIMEOUT_MS";
const REQUEST_TIMEOUT_MS_ENV: &str = "DISTD_QUIC_REQUEST_TIMEOUT_MS";
const WRITE_TIMEOUT_MS_ENV: &str = "DISTD_QUIC_WRITE_TIMEOUT_MS";
const STREAM_LIFETIME_MS_ENV: &str = "DISTD_QUIC_STREAM_LIFETIME_MS";

#[derive(Clone, Debug)]
pub struct QuicResourceSettings {
    pub pending_global: usize,
    pub pending_per_connection: usize,
    pub pending_budget_bytes: usize,
    pub active_global: usize,
    pub active_per_connection: usize,
    pub active_budget_bytes: usize,
    pub handshake_timeout: Duration,
    pub connection_idle_timeout: Duration,
    pub prelude_timeout: Duration,
    pub uuid_timeout: Duration,
    pub request_timeout: Duration,
    pub write_timeout: Duration,
    pub stream_lifetime: Duration,
    #[cfg(test)]
    post_auth_prefix_probe: bool,
}

impl Default for QuicResourceSettings {
    fn default() -> Self {
        Self {
            pending_global: MAX_PENDING_GLOBAL,
            pending_per_connection: MAX_PENDING_PER_CONNECTION,
            pending_budget_bytes: MAX_PENDING_BUDGET_BYTES,
            active_global: MAX_ACTIVE_GLOBAL,
            active_per_connection: MAX_ACTIVE_PER_CONNECTION,
            active_budget_bytes: MAX_ACTIVE_BUDGET_BYTES,
            handshake_timeout: Duration::from_secs(10),
            connection_idle_timeout: Duration::from_secs(60),
            prelude_timeout: Duration::from_secs(5),
            uuid_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            stream_lifetime: Duration::from_secs(15 * 60),
            #[cfg(test)]
            post_auth_prefix_probe: false,
        }
    }
}

impl QuicResourceSettings {
    pub fn from_environment() -> Result<Self, String> {
        Self::from_lookup(environment_value)
    }

    fn from_lookup(
        mut lookup: impl FnMut(&str) -> Result<Option<String>, String>,
    ) -> Result<Self, String> {
        let defaults = Self::default();
        let settings = Self {
            pending_global: lookup_usize(&mut lookup, PENDING_GLOBAL_ENV, defaults.pending_global)?,
            pending_per_connection: lookup_usize(
                &mut lookup,
                PENDING_PER_CONNECTION_ENV,
                defaults.pending_per_connection,
            )?,
            pending_budget_bytes: lookup_mib(
                &mut lookup,
                PENDING_BUDGET_MIB_ENV,
                defaults.pending_budget_bytes,
            )?,
            active_global: lookup_usize(&mut lookup, ACTIVE_GLOBAL_ENV, defaults.active_global)?,
            active_per_connection: lookup_usize(
                &mut lookup,
                ACTIVE_PER_CONNECTION_ENV,
                defaults.active_per_connection,
            )?,
            active_budget_bytes: lookup_mib(
                &mut lookup,
                ACTIVE_BUDGET_MIB_ENV,
                defaults.active_budget_bytes,
            )?,
            handshake_timeout: lookup_duration(
                &mut lookup,
                HANDSHAKE_TIMEOUT_MS_ENV,
                defaults.handshake_timeout,
            )?,
            connection_idle_timeout: lookup_duration(
                &mut lookup,
                CONNECTION_IDLE_TIMEOUT_MS_ENV,
                defaults.connection_idle_timeout,
            )?,
            prelude_timeout: lookup_duration(
                &mut lookup,
                PRELUDE_TIMEOUT_MS_ENV,
                defaults.prelude_timeout,
            )?,
            uuid_timeout: lookup_duration(&mut lookup, UUID_TIMEOUT_MS_ENV, defaults.uuid_timeout)?,
            request_timeout: lookup_duration(
                &mut lookup,
                REQUEST_TIMEOUT_MS_ENV,
                defaults.request_timeout,
            )?,
            write_timeout: lookup_duration(
                &mut lookup,
                WRITE_TIMEOUT_MS_ENV,
                defaults.write_timeout,
            )?,
            stream_lifetime: lookup_duration(
                &mut lookup,
                STREAM_LIFETIME_MS_ENV,
                defaults.stream_lifetime,
            )?,
            #[cfg(test)]
            post_auth_prefix_probe: false,
        };
        settings.validate()?;
        Ok(settings)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_count(PENDING_GLOBAL_ENV, self.pending_global, MAX_PENDING_GLOBAL)?;
        validate_count(
            PENDING_PER_CONNECTION_ENV,
            self.pending_per_connection,
            MAX_PENDING_PER_CONNECTION,
        )?;
        validate_count(ACTIVE_GLOBAL_ENV, self.active_global, MAX_ACTIVE_GLOBAL)?;
        validate_count(
            ACTIVE_PER_CONNECTION_ENV,
            self.active_per_connection,
            MAX_ACTIVE_PER_CONNECTION,
        )?;
        if self.pending_per_connection > self.pending_global {
            return Err(format!(
                "{PENDING_PER_CONNECTION_ENV} must not exceed {PENDING_GLOBAL_ENV}"
            ));
        }
        if self.active_per_connection > self.active_global {
            return Err(format!(
                "{ACTIVE_PER_CONNECTION_ENV} must not exceed {ACTIVE_GLOBAL_ENV}"
            ));
        }
        validate_budget(
            PENDING_BUDGET_MIB_ENV,
            self.pending_budget_bytes,
            MAX_PENDING_BUDGET_BYTES,
            self.pending_global,
            PENDING_RESERVATION_BYTES,
        )?;
        validate_budget(
            ACTIVE_BUDGET_MIB_ENV,
            self.active_budget_bytes,
            MAX_ACTIVE_BUDGET_BYTES,
            self.active_global,
            ACTIVE_RESERVATION_BYTES,
        )?;
        for (name, value) in [
            (HANDSHAKE_TIMEOUT_MS_ENV, self.handshake_timeout),
            (CONNECTION_IDLE_TIMEOUT_MS_ENV, self.connection_idle_timeout),
            (PRELUDE_TIMEOUT_MS_ENV, self.prelude_timeout),
            (UUID_TIMEOUT_MS_ENV, self.uuid_timeout),
            (REQUEST_TIMEOUT_MS_ENV, self.request_timeout),
            (WRITE_TIMEOUT_MS_ENV, self.write_timeout),
            (STREAM_LIFETIME_MS_ENV, self.stream_lifetime),
        ] {
            if value.is_zero() || value > MAX_TIMEOUT {
                return Err(format!("{name} must be between 1 ms and 86400000 ms"));
            }
        }
        for (name, value) in [
            (HANDSHAKE_TIMEOUT_MS_ENV, self.handshake_timeout),
            (CONNECTION_IDLE_TIMEOUT_MS_ENV, self.connection_idle_timeout),
            (PRELUDE_TIMEOUT_MS_ENV, self.prelude_timeout),
            (UUID_TIMEOUT_MS_ENV, self.uuid_timeout),
            (REQUEST_TIMEOUT_MS_ENV, self.request_timeout),
            (WRITE_TIMEOUT_MS_ENV, self.write_timeout),
        ] {
            if value > self.stream_lifetime {
                return Err(format!(
                    "{STREAM_LIFETIME_MS_ENV} must not be shorter than {name}"
                ));
            }
        }
        Ok(())
    }
}

fn environment_value(name: &str) -> Result<Option<String>, String> {
    match std::env::var_os(name) {
        None => Ok(None),
        Some(value) => value
            .into_string()
            .map(Some)
            .map_err(|_| format!("{name} must contain valid UTF-8")),
    }
}

fn lookup_usize(
    lookup: &mut impl FnMut(&str) -> Result<Option<String>, String>,
    name: &str,
    default: usize,
) -> Result<usize, String> {
    lookup(name)?.map_or(Ok(default), |value| {
        if value.is_empty() {
            return Err(format!("{name} must not be empty"));
        }
        value
            .parse::<usize>()
            .map_err(|_| format!("{name} must be a positive decimal integer"))
    })
}

fn lookup_mib(
    lookup: &mut impl FnMut(&str) -> Result<Option<String>, String>,
    name: &str,
    default_bytes: usize,
) -> Result<usize, String> {
    let mib = lookup_usize(lookup, name, default_bytes / (1024 * 1024))?;
    mib.checked_mul(1024 * 1024)
        .ok_or_else(|| format!("{name} is too large"))
}

fn lookup_duration(
    lookup: &mut impl FnMut(&str) -> Result<Option<String>, String>,
    name: &str,
    default: Duration,
) -> Result<Duration, String> {
    let milliseconds = lookup(name)?.map_or(Ok(default.as_millis() as u64), |value| {
        if value.is_empty() {
            return Err(format!("{name} must not be empty"));
        }
        value
            .parse::<u64>()
            .map_err(|_| format!("{name} must be a positive decimal millisecond value"))
    })?;
    Ok(Duration::from_millis(milliseconds))
}

fn validate_count(name: &str, value: usize, maximum: usize) -> Result<(), String> {
    if value == 0 || value > maximum {
        return Err(format!("{name} must be between 1 and {maximum}"));
    }
    Ok(())
}

fn validate_budget(
    name: &str,
    bytes: usize,
    maximum: usize,
    count: usize,
    reservation: usize,
) -> Result<(), String> {
    if bytes == 0 || bytes > maximum {
        return Err(format!(
            "{name} must be between 1 MiB and {} MiB",
            maximum / 1024 / 1024
        ));
    }
    let required = count
        .checked_mul(reservation)
        .ok_or_else(|| format!("{name} admission equation overflows"))?;
    if required > bytes {
        return Err(format!(
            "{name} is too small for its configured admission count"
        ));
    }
    Ok(())
}

#[cfg(test)]
fn reservation_bytes(count: usize, reservation: usize) -> usize {
    count
        .checked_mul(reservation)
        .expect("validated admission count must fit its fixed byte reservation")
}

#[derive(Debug, Default)]
struct AdmissionCounters {
    handshakes: AtomicUsize,
    pending: AtomicUsize,
    pending_peak: AtomicUsize,
    active: AtomicUsize,
    active_peak: AtomicUsize,
    #[cfg(test)]
    post_auth_prefix_reads: AtomicUsize,
    #[cfg(test)]
    failure_events: std::sync::Mutex<Vec<(QuicPhase, StreamFailureKind)>>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
struct AdmissionSnapshot {
    handshakes: usize,
    pending: usize,
    pending_peak: usize,
    pending_bytes: usize,
    pending_bytes_peak: usize,
    active: usize,
    active_peak: usize,
    active_bytes: usize,
    active_bytes_peak: usize,
    post_auth_prefix_reads: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct QuicAdmission {
    settings: QuicResourceSettings,
    global_pending_count: Arc<Semaphore>,
    global_active_count: Arc<Semaphore>,
    counters: Arc<AdmissionCounters>,
}

#[derive(Debug)]
struct ConnectionAdmission {
    pending_count: Arc<Semaphore>,
    active_count: Arc<Semaphore>,
}

struct ConnectionContext<T>
where
    T: ChunkStorage + Sync + Send,
{
    server: Server<T>,
    global_admission: QuicAdmission,
    local_admission: ConnectionAdmission,
}

#[derive(Debug)]
struct HandshakeGuard {
    _global_count: OwnedSemaphorePermit,
    counters: Arc<AdmissionCounters>,
}

#[derive(Debug)]
struct PendingGuard {
    _global_count: OwnedSemaphorePermit,
    _connection_count: OwnedSemaphorePermit,
    counters: Arc<AdmissionCounters>,
}

#[derive(Debug)]
struct ActiveGuard {
    _global_count: OwnedSemaphorePermit,
    _connection_count: OwnedSemaphorePermit,
    counters: Arc<AdmissionCounters>,
}

impl Drop for HandshakeGuard {
    fn drop(&mut self) {
        self.counters.handshakes.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.counters.pending.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.counters.active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl QuicAdmission {
    pub(crate) fn new(settings: QuicResourceSettings) -> Result<Self, String> {
        settings.validate()?;
        Ok(Self {
            global_pending_count: Arc::new(Semaphore::new(settings.pending_global)),
            global_active_count: Arc::new(Semaphore::new(settings.active_global)),
            counters: Arc::new(AdmissionCounters::default()),
            settings,
        })
    }

    fn connection(&self) -> ConnectionAdmission {
        ConnectionAdmission {
            pending_count: Arc::new(Semaphore::new(self.settings.pending_per_connection)),
            active_count: Arc::new(Semaphore::new(self.settings.active_per_connection)),
        }
    }

    fn try_handshake(&self) -> Result<HandshakeGuard, StreamFailure> {
        let global_count = self
            .global_pending_count
            .clone()
            .try_acquire_owned()
            .map_err(|_| StreamFailure::overloaded(QuicPhase::Handshake))?;
        self.counters.handshakes.fetch_add(1, Ordering::Relaxed);
        Ok(HandshakeGuard {
            _global_count: global_count,
            counters: self.counters.clone(),
        })
    }

    fn try_pending(&self, connection: &ConnectionAdmission) -> Result<PendingGuard, StreamFailure> {
        let connection_count = connection
            .pending_count
            .clone()
            .try_acquire_owned()
            .map_err(|_| StreamFailure::overloaded(QuicPhase::Admission))?;
        let global_count = self
            .global_pending_count
            .clone()
            .try_acquire_owned()
            .map_err(|_| StreamFailure::overloaded(QuicPhase::Admission))?;
        let pending = self.counters.pending.fetch_add(1, Ordering::Relaxed) + 1;
        self.counters
            .pending_peak
            .fetch_max(pending, Ordering::Relaxed);
        Ok(PendingGuard {
            _global_count: global_count,
            _connection_count: connection_count,
            counters: self.counters.clone(),
        })
    }

    fn try_active(&self, connection: &ConnectionAdmission) -> Result<ActiveGuard, StreamFailure> {
        let connection_count = connection
            .active_count
            .clone()
            .try_acquire_owned()
            .map_err(|_| StreamFailure::overloaded(QuicPhase::Admission))?;
        let global_count = self
            .global_active_count
            .clone()
            .try_acquire_owned()
            .map_err(|_| StreamFailure::overloaded(QuicPhase::Admission))?;
        let active = self.counters.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.counters
            .active_peak
            .fetch_max(active, Ordering::Relaxed);
        Ok(ActiveGuard {
            _global_count: global_count,
            _connection_count: connection_count,
            counters: self.counters.clone(),
        })
    }

    #[cfg(test)]
    fn snapshot(&self) -> AdmissionSnapshot {
        let pending = self.counters.pending.load(Ordering::Relaxed);
        let pending_peak = self
            .counters
            .pending_peak
            .load(Ordering::Relaxed)
            .max(pending);
        let active = self.counters.active.load(Ordering::Relaxed);
        let active_peak = self
            .counters
            .active_peak
            .load(Ordering::Relaxed)
            .max(active);
        AdmissionSnapshot {
            handshakes: self.counters.handshakes.load(Ordering::Relaxed),
            pending,
            pending_peak,
            pending_bytes: reservation_bytes(pending, PENDING_RESERVATION_BYTES),
            pending_bytes_peak: reservation_bytes(pending_peak, PENDING_RESERVATION_BYTES),
            active,
            active_peak,
            active_bytes: reservation_bytes(active, ACTIVE_RESERVATION_BYTES),
            active_bytes_peak: reservation_bytes(active_peak, ACTIVE_RESERVATION_BYTES),
            post_auth_prefix_reads: self.counters.post_auth_prefix_reads.load(Ordering::Relaxed),
        }
    }

    #[inline(always)]
    fn record_post_auth_prefix_read<const PROBE: bool>(&self) {
        #[cfg(test)]
        if PROBE {
            self.counters
                .post_auth_prefix_reads
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    fn post_auth_prefix_probe_enabled(&self) -> bool {
        self.settings.post_auth_prefix_probe
    }

    #[cfg(test)]
    fn failure_events(&self) -> Vec<(QuicPhase, StreamFailureKind)> {
        self.counters
            .failure_events
            .lock()
            .expect("failure event lock poisoned")
            .clone()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuicPhase {
    Handshake,
    ConnectionIdle,
    Admission,
    Prelude,
    Uuid,
    Registration,
    Request,
    Possession,
    ResponseWrite,
    Lifetime,
    Internal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamFailureKind {
    Overloaded,
    Timeout,
    Protocol,
    Internal,
}

#[derive(Clone, Copy, Debug)]
struct StreamFailure {
    phase: QuicPhase,
    kind: StreamFailureKind,
}

impl StreamFailure {
    const fn overloaded(phase: QuicPhase) -> Self {
        Self {
            phase,
            kind: StreamFailureKind::Overloaded,
        }
    }

    const fn timeout(phase: QuicPhase) -> Self {
        Self {
            phase,
            kind: StreamFailureKind::Timeout,
        }
    }

    const fn protocol(phase: QuicPhase) -> Self {
        Self {
            phase,
            kind: StreamFailureKind::Protocol,
        }
    }

    const fn internal(phase: QuicPhase) -> Self {
        Self {
            phase,
            kind: StreamFailureKind::Internal,
        }
    }

    const fn reset_code(self) -> u32 {
        match self.kind {
            StreamFailureKind::Overloaded => QUIC_STREAM_OVERLOADED_CODE,
            StreamFailureKind::Timeout => QUIC_STREAM_TIMEOUT_CODE,
            StreamFailureKind::Protocol => QUIC_STREAM_PROTOCOL_CODE,
            StreamFailureKind::Internal => QUIC_STREAM_INTERNAL_CODE,
        }
    }

    const fn reason(self) -> &'static str {
        match self.kind {
            StreamFailureKind::Overloaded => "overloaded",
            StreamFailureKind::Timeout => "timeout",
            StreamFailureKind::Protocol => "protocol",
            StreamFailureKind::Internal => "internal",
        }
    }
}

async fn result_at<F, T, E>(
    deadline: Instant,
    absolute: Instant,
    phase: QuicPhase,
    future: F,
) -> Result<T, StreamFailure>
where
    F: Future<Output = Result<T, E>>,
{
    match timeout_at(deadline, future).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) => Err(StreamFailure::protocol(phase)),
        Err(_) if deadline == absolute => Err(StreamFailure::timeout(QuicPhase::Lifetime)),
        Err(_) => Err(StreamFailure::timeout(phase)),
    }
}

fn ensure_stream_alive(absolute: Instant) -> Result<(), StreamFailure> {
    if Instant::now() >= absolute {
        Err(StreamFailure::timeout(QuicPhase::Lifetime))
    } else {
        Ok(())
    }
}

fn fixed_phase_deadline(
    absolute: Instant,
    duration: Duration,
    phase: QuicPhase,
) -> Result<Instant, StreamFailure> {
    let now = Instant::now();
    if now >= absolute {
        return Err(StreamFailure::timeout(QuicPhase::Lifetime));
    }
    let deadline = absolute.min(now + duration);
    if deadline <= now {
        return Err(StreamFailure::timeout(phase));
    }
    Ok(deadline)
}

fn ensure_phase_alive(
    deadline: Instant,
    absolute: Instant,
    phase: QuicPhase,
) -> Result<(), StreamFailure> {
    if Instant::now() < deadline {
        Ok(())
    } else if deadline == absolute {
        Err(StreamFailure::timeout(QuicPhase::Lifetime))
    } else {
        Err(StreamFailure::timeout(phase))
    }
}

fn report_failure(_admission: &QuicAdmission, failure: StreamFailure) {
    #[cfg(test)]
    _admission
        .counters
        .failure_events
        .lock()
        .expect("failure event lock poisoned")
        .push((failure.phase, failure.kind));
    tracing::warn!(
        phase = ?failure.phase,
        reason = failure.reason(),
        "QUIC request terminated"
    );
}

async fn read_message_bounded<R, M>(
    reader: &mut R,
    maximum: usize,
    deadline: Instant,
    absolute: Instant,
    phase: QuicPhase,
) -> Result<M, StreamFailure>
where
    R: tokio::io::AsyncRead + Unpin,
    M: Message + Default,
{
    result_at(
        deadline,
        absolute,
        phase,
        read_length_delimited_async_with_limit(reader, maximum),
    )
    .await
}

async fn read_prelude(
    send: &mut SendStream,
    recv: &mut RecvStream,
    absolute: Instant,
    settings: &QuicResourceSettings,
) -> Result<Option<TransportOp>, StreamFailure> {
    let deadline = fixed_phase_deadline(absolute, settings.prelude_timeout, QuicPhase::Prelude)?;
    let declaration = match timeout_at(deadline, read_wire_preface_async(recv)).await {
        Ok(Ok(declaration)) => declaration,
        Ok(Err(error)) => {
            let status = match error {
                FrameError::WireCompatibility(source) => source.rejection_status(),
                FrameError::IoError(source)
                    if source.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    WireRejectionStatus::MalformedPreface
                }
                _ => return Err(StreamFailure::protocol(QuicPhase::Prelude)),
            };
            result_at(
                deadline,
                absolute,
                QuicPhase::Prelude,
                write_wire_rejection_async(send, WireRejection::current(status)),
            )
            .await?;
            finish_send(send)
                .await
                .map_err(|_| StreamFailure::internal(QuicPhase::Prelude))?;
            let _ = recv.stop(quinn::VarInt::from_u32(QUIC_STREAM_PROTOCOL_CODE));
            return Ok(None);
        }
        Err(_) if deadline == absolute => {
            return Err(StreamFailure::timeout(QuicPhase::Lifetime));
        }
        Err(_) => return Err(StreamFailure::timeout(QuicPhase::Prelude)),
    };
    debug_assert_eq!(
        declaration,
        distd_core::transport::WireVersionDeclaration::CURRENT
    );
    let op = result_at(
        deadline,
        absolute,
        QuicPhase::Prelude,
        read_transport_op_async(recv),
    )
    .await?;
    Ok(Some(op))
}

async fn write_message_bounded<W, M>(
    writer: &mut W,
    message: &M,
    deadline: Instant,
    absolute: Instant,
) -> Result<(), StreamFailure>
where
    W: tokio::io::AsyncWrite + Unpin,
    M: Message,
{
    write_response_at(
        deadline,
        absolute,
        write_length_delimited_async(writer, message),
    )
    .await
}

async fn write_response_at<F, E>(
    deadline: Instant,
    absolute: Instant,
    future: F,
) -> Result<(), StreamFailure>
where
    F: Future<Output = Result<(), E>>,
{
    result_at(deadline, absolute, QuicPhase::ResponseWrite, future).await
}

fn stop_and_reset(
    send: &mut SendStream,
    recv: &mut RecvStream,
    admission: &QuicAdmission,
    failure: StreamFailure,
) {
    let code = quinn::VarInt::from_u32(failure.reset_code());
    let _ = recv.stop(code);
    let _ = send.reset(code);
    report_failure(admission, failure);
}

async fn await_admitted_handshake<F, T, E>(
    connecting: F,
    handshake: HandshakeGuard,
    deadline: Instant,
    admission: &QuicAdmission,
) -> Result<T, StreamFailure>
where
    F: IntoFuture<Output = Result<T, E>>,
{
    let result = timeout_at(deadline, connecting.into_future()).await;
    drop(handshake);
    match result {
        Ok(Ok(connection)) => Ok(connection),
        Ok(Err(_)) => {
            let failure = StreamFailure::protocol(QuicPhase::Handshake);
            report_failure(admission, failure);
            Err(failure)
        }
        Err(_) => {
            let failure = StreamFailure::timeout(QuicPhase::Handshake);
            report_failure(admission, failure);
            Err(failure)
        }
    }
}

fn make_server_config() -> Result<quinn::ServerConfig, ServerError> {
    let (cert_der, key_der) = server_identity()?;
    tracing::info!(
        fingerprint = %blake3::hash(cert_der.as_ref()).to_hex(),
        "loaded QUIC server identity"
    );
    let mut config = quinn::ServerConfig::with_single_cert(vec![cert_der], key_der)
        .map_err(|err| ServerError::Quic(err.to_string()))?;

    let mut transport = quinn::TransportConfig::default();
    transport.receive_window(quinn::VarInt::from_u32(64 * 1024 * 1024));
    transport.stream_receive_window(quinn::VarInt::from_u32(32 * 1024 * 1024));
    transport.send_window(64 * 1024 * 1024);
    transport.keep_alive_interval(Some(std::time::Duration::from_secs(5)));
    transport.max_concurrent_bidi_streams(quinn::VarInt::from_u32(32));
    transport.max_concurrent_uni_streams(quinn::VarInt::from_u32(0));
    transport.initial_rtt(std::time::Duration::from_millis(1));
    let mut bbr = quinn::congestion::BbrConfig::default();
    bbr.initial_window(1024 * 1024);
    transport.congestion_controller_factory(std::sync::Arc::new(bbr));

    config.transport_config(std::sync::Arc::new(transport));
    Ok(config)
}

#[cfg(test)]
fn server_identity() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), ServerError> {
    generate_server_identity()
}

#[cfg(not(test))]
fn server_identity() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), ServerError> {
    let state_dir = std::env::var("DISTD_SERVER_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| distd_core::utils::settings::cache_dir().join("server-state"));
    std::fs::create_dir_all(&state_dir)?;
    let cert_path = state_dir.join("quic-cert.der");
    let key_path = state_dir.join("quic-key.der");

    if cert_path.is_file() && key_path.is_file() {
        let cert = CertificateDer::from(std::fs::read(cert_path)?);
        let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(std::fs::read(key_path)?));
        return Ok((cert, key));
    }

    let (cert, key) = generate_server_identity()?;
    atomic_write(&cert_path, cert.as_ref())?;
    let secret = key.secret_der();
    atomic_write(&key_path, secret)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok((cert, key))
}

fn generate_server_identity(
) -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), ServerError> {
    let certified =
        generate_simple_self_signed(vec![String::from("localhost"), String::from("127.0.0.1")])
            .map_err(|err| ServerError::Quic(err.to_string()))?;
    Ok((
        CertificateDer::from(certified.cert.der().clone()),
        PrivateKeyDer::from(PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der())),
    ))
}

#[cfg(not(test))]
fn atomic_write(path: &std::path::Path, contents: &[u8]) -> Result<(), std::io::Error> {
    let temporary = path.with_extension("tmp");
    std::fs::write(&temporary, contents)?;
    std::fs::rename(temporary, path)
}

async fn finish_send(send: &mut SendStream) -> Result<(), ServerError> {
    send.finish()
        .map_err(|err| ServerError::Quic(err.to_string()))?;
    Ok(())
}

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    async fn handle_quic_stream<const PROBE: bool>(
        &self,
        remote_addr: SocketAddr,
        send: &mut SendStream,
        recv: &mut RecvStream,
        pending: PendingGuard,
        connection_admission: &ConnectionAdmission,
        admission: &QuicAdmission,
        absolute: Instant,
    ) -> Result<(), StreamFailure> {
        let settings = &admission.settings;
        let Some(op) = read_prelude(send, recv, absolute, settings).await? else {
            drop(pending);
            report_failure(admission, StreamFailure::protocol(QuicPhase::Prelude));
            return Ok(());
        };
        let uuid_deadline = fixed_phase_deadline(absolute, settings.uuid_timeout, QuicPhase::Uuid)?;
        let client_uuid = result_at(
            uuid_deadline,
            absolute,
            QuicPhase::Uuid,
            read_optional_uuid_async(recv),
        )
        .await?;

        match op {
            TransportOp::Register => {
                let registration_deadline = fixed_phase_deadline(
                    absolute,
                    settings.request_timeout,
                    QuicPhase::Registration,
                )?;
                let request: ClientRegister = read_message_bounded(
                    recv,
                    REGISTER_FRAME_MAX_BYTES,
                    registration_deadline,
                    absolute,
                    QuicPhase::Registration,
                )
                .await?;
                if client_uuid.is_some() && request.uuid.is_none() {
                    tracing::debug!("QUIC register stream carried client auth UUID");
                }
                let active = admission.try_active(connection_admission)?;
                drop(pending);
                let response = result_at(
                    registration_deadline,
                    absolute,
                    QuicPhase::Registration,
                    self.register_response(remote_addr, request),
                )
                .await
                .map_err(|failure| match failure.kind {
                    StreamFailureKind::Protocol => StreamFailure::internal(QuicPhase::Registration),
                    _ => failure,
                })?;
                let response_deadline = fixed_phase_deadline(
                    absolute,
                    settings.write_timeout,
                    QuicPhase::ResponseWrite,
                )?;
                write_message_bounded(send, &response, response_deadline, absolute).await?;
                ensure_phase_alive(response_deadline, absolute, QuicPhase::ResponseWrite)?;
                finish_send(send)
                    .await
                    .map_err(|_| StreamFailure::internal(QuicPhase::ResponseWrite))?;
                drop(active);
                Ok(())
            }
            TransportOp::Fetch => {
                result_at(
                    uuid_deadline,
                    absolute,
                    QuicPhase::Uuid,
                    self.authenticate_client_uuid(client_uuid),
                )
                .await?;
                let active = admission.try_active(connection_admission)?;
                drop(pending);
                admission.record_post_auth_prefix_read::<PROBE>();
                let request_deadline =
                    fixed_phase_deadline(absolute, settings.request_timeout, QuicPhase::Request)?;
                let request: ClientKeepAlive = read_message_bounded(
                    recv,
                    MAX_FRAME_PAYLOAD_BYTES,
                    request_deadline,
                    absolute,
                    QuicPhase::Request,
                )
                .await?;
                let response = result_at(
                    request_deadline,
                    absolute,
                    QuicPhase::Request,
                    self.fetch_response(request),
                )
                .await
                .map_err(|failure| match failure.kind {
                    StreamFailureKind::Protocol => StreamFailure::internal(QuicPhase::Request),
                    _ => failure,
                })?;
                let response_deadline = fixed_phase_deadline(
                    absolute,
                    settings.write_timeout,
                    QuicPhase::ResponseWrite,
                )?;
                write_message_bounded(send, &response, response_deadline, absolute).await?;
                ensure_phase_alive(response_deadline, absolute, QuicPhase::ResponseWrite)?;
                finish_send(send)
                    .await
                    .map_err(|_| StreamFailure::internal(QuicPhase::ResponseWrite))?;
                drop(active);
                Ok(())
            }
            TransportOp::Sync => {
                result_at(
                    uuid_deadline,
                    absolute,
                    QuicPhase::Uuid,
                    self.authenticate_client_uuid(client_uuid),
                )
                .await?;
                let active = admission.try_active(connection_admission)?;
                drop(pending);
                admission.record_post_auth_prefix_read::<PROBE>();
                let request_deadline =
                    fixed_phase_deadline(absolute, settings.request_timeout, QuicPhase::Request)?;
                let manifest_request: ManifestRequest = read_message_bounded(
                    recv,
                    MAX_FRAME_PAYLOAD_BYTES,
                    request_deadline,
                    absolute,
                    QuicPhase::Request,
                )
                .await?;
                let (manifest_response, item) = result_at(
                    request_deadline,
                    absolute,
                    QuicPhase::Request,
                    self.sync_manifest_response(manifest_request),
                )
                .await
                .map_err(|failure| match failure.kind {
                    StreamFailureKind::Protocol => StreamFailure::internal(QuicPhase::Request),
                    _ => failure,
                })?;
                let manifest_write_deadline = fixed_phase_deadline(
                    absolute,
                    settings.write_timeout,
                    QuicPhase::ResponseWrite,
                )?;
                let manifest_frame = tokio::task::spawn_blocking(move || {
                    encode_length_delimited(&SyncMessage {
                        msg: Some(Msg::ManifestResponse(manifest_response)),
                    })
                })
                .await
                .map_err(|_| StreamFailure::internal(QuicPhase::Internal))?
                .map_err(|_| StreamFailure::internal(QuicPhase::Internal))?;
                write_response_at(
                    manifest_write_deadline,
                    absolute,
                    send.write_all(&manifest_frame),
                )
                .await?;
                let possession_deadline = fixed_phase_deadline(
                    absolute,
                    settings.request_timeout,
                    QuicPhase::Possession,
                )?;
                let possession: PossessionBitfield = read_message_bounded(
                    recv,
                    MAX_FRAME_PAYLOAD_BYTES,
                    possession_deadline,
                    absolute,
                    QuicPhase::Possession,
                )
                .await?;
                let mut iter_state = self
                    .prepare_sync_chunks(&item, &possession)
                    .map_err(|_| StreamFailure::protocol(QuicPhase::Possession))?;
                ensure_phase_alive(possession_deadline, absolute, QuicPhase::Possession)?;
                let primary_path = item.metadata.path.clone();
                let root_hash = iter_state.root_hash;
                let sync_start = std::time::Instant::now();
                let mut batch_build_ns: u64 = 0;
                let mut encode_ns: u64 = 0;
                let mut write_ns: u64 = 0;
                let mut batch_count: u32 = 0;
                let mut total_bytes: u64 = 0;
                loop {
                    ensure_stream_alive(absolute)?;
                    let t0 = std::time::Instant::now();
                    let batch = build_one_batch(
                        &iter_state.missing,
                        &mut iter_state.cursor,
                        Some(primary_path.as_path()),
                        iter_state.likely_precompressed,
                        &mut |idx| self.storage().get_chunk_by_index(&root_hash, idx),
                    )
                    .map_err(|_| StreamFailure::internal(QuicPhase::Internal))?;
                    batch_build_ns += t0.elapsed().as_nanos() as u64;
                    ensure_stream_alive(absolute)?;
                    let Some(batch) = batch else { break };
                    batch_count += 1;
                    let t1 = std::time::Instant::now();
                    let payload_len = prost::Message::encoded_len(&batch);
                    if payload_len > MAX_FRAME_PAYLOAD_BYTES {
                        return Err(StreamFailure::internal(QuicPhase::Internal));
                    }
                    let frame_len = u32::try_from(payload_len)
                        .map_err(|_| StreamFailure::internal(QuicPhase::Internal))?;
                    let mut buf = Vec::with_capacity(payload_len);
                    prost::Message::encode(&batch, &mut buf)
                        .map_err(|_| StreamFailure::internal(QuicPhase::Internal))?;
                    encode_ns += t1.elapsed().as_nanos() as u64;
                    total_bytes += payload_len as u64;
                    ensure_stream_alive(absolute)?;
                    let t2 = std::time::Instant::now();
                    let write_deadline = absolute.min(Instant::now() + settings.write_timeout);
                    write_response_at(
                        write_deadline,
                        absolute,
                        send.write_all(&frame_len.to_be_bytes()),
                    )
                    .await?;
                    write_response_at(write_deadline, absolute, send.write_all(&buf)).await?;
                    write_ns += t2.elapsed().as_nanos() as u64;
                }
                let total_ms = sync_start.elapsed().as_millis();
                tracing::info!(
                    batch_count,
                    total_ms,
                    build_ms = batch_build_ns / 1_000_000,
                    encode_ms = encode_ns / 1_000_000,
                    write_ms = write_ns / 1_000_000,
                    total_bytes,
                    "sync_server_timing"
                );
                ensure_stream_alive(absolute)?;
                finish_send(send)
                    .await
                    .map_err(|_| StreamFailure::internal(QuicPhase::ResponseWrite))?;
                drop(active);
                Ok(())
            }
        }
    }
}

impl<T> ConnectionContext<T>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    fn new(server: Server<T>, global_admission: QuicAdmission) -> Self {
        let local_admission = global_admission.connection();
        Self {
            server,
            global_admission,
            local_admission,
        }
    }

    async fn run_stream<const PROBE: bool>(
        self: Arc<Self>,
        remote_addr: SocketAddr,
        mut send: SendStream,
        mut recv: RecvStream,
        pending: PendingGuard,
        absolute: Instant,
    ) {
        if let Err(failure) = self
            .server
            .handle_quic_stream::<PROBE>(
                remote_addr,
                &mut send,
                &mut recv,
                pending,
                &self.local_admission,
                &self.global_admission,
                absolute,
            )
            .await
        {
            stop_and_reset(&mut send, &mut recv, &self.global_admission, failure);
        }
    }

    async fn serve_connection<const PROBE: bool>(self: Arc<Self>, connection: quinn::Connection) {
        let mut streams = JoinSet::new();
        loop {
            let accepted = if streams.is_empty() {
                match timeout(
                    self.global_admission.settings.connection_idle_timeout,
                    connection.accept_bi(),
                )
                .await
                {
                    Ok(accepted) => accepted,
                    Err(_) => {
                        report_failure(
                            &self.global_admission,
                            StreamFailure::timeout(QuicPhase::ConnectionIdle),
                        );
                        connection.close(
                            quinn::VarInt::from_u32(QUIC_STREAM_TIMEOUT_CODE),
                            b"application connection idle timeout",
                        );
                        break;
                    }
                }
            } else {
                tokio::select! {
                    accepted = connection.accept_bi() => accepted,
                    _ = streams.join_next() => continue,
                }
            };

            let (mut send, mut recv) = match accepted {
                Ok(streams) => streams,
                Err(_) => break,
            };
            let pending = match self.global_admission.try_pending(&self.local_admission) {
                Ok(pending) => pending,
                Err(failure) => {
                    stop_and_reset(&mut send, &mut recv, &self.global_admission, failure);
                    continue;
                }
            };
            let absolute = Instant::now() + self.global_admission.settings.stream_lifetime;
            let stream_context = Arc::clone(&self);
            let remote_addr = connection.remote_address();
            streams.spawn(async move {
                stream_context
                    .run_stream::<PROBE>(remote_addr, send, recv, pending, absolute)
                    .await;
            });
        }
        streams.abort_all();
        while streams.join_next().await.is_some() {}
    }
}

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    async fn serve_quic_endpoint_specialized<const PROBE: bool>(
        self,
        endpoint: Endpoint,
        admission: QuicAdmission,
    ) -> Result<(), ServerError> {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let handshake = match admission.try_handshake() {
                        Ok(handshake) => handshake,
                        Err(failure) => {
                            incoming.refuse();
                            report_failure(&admission, failure);
                            continue;
                        }
                    };
                    let handshake_deadline =
                        Instant::now() + admission.settings.handshake_timeout;
                    let server = self.clone();
                    let connection_admission = admission.clone();
                    connections.spawn(async move {
                        match await_admitted_handshake(
                            incoming,
                            handshake,
                            handshake_deadline,
                            &connection_admission,
                        )
                        .await
                        {
                            Ok(connection) => {
                                Arc::new(ConnectionContext::new(server, connection_admission))
                                    .serve_connection::<PROBE>(connection)
                                    .await;
                            }
                            Err(_) => {}
                        }
                    });
                }
                _ = connections.join_next(), if !connections.is_empty() => {}
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        Ok(())
    }

    pub(crate) async fn serve_quic_endpoint(
        self,
        endpoint: Endpoint,
        admission: QuicAdmission,
    ) -> Result<(), ServerError> {
        self.serve_quic_endpoint_specialized::<false>(endpoint, admission)
            .await
    }

    pub async fn serve_quic_with_settings(
        self,
        bind_addr: SocketAddr,
        settings: QuicResourceSettings,
    ) -> Result<(), ServerError> {
        let admission = QuicAdmission::new(settings).map_err(ServerError::Quic)?;
        let endpoint = Endpoint::server(make_server_config()?, bind_addr)
            .map_err(|err| ServerError::Quic(err.to_string()))?;
        tracing::info!("listening on {} for QUIC", bind_addr);
        self.serve_quic_endpoint(endpoint, admission).await
    }

    pub async fn serve_quic(self, bind_addr: SocketAddr) -> Result<(), ServerError> {
        let settings = QuicResourceSettings::from_environment().map_err(ServerError::Quic)?;
        self.serve_quic_with_settings(bind_addr, settings).await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        make_server_config, QuicAdmission, QuicResourceSettings, ACTIVE_RESERVATION_BYTES,
        MAX_ACTIVE_BUDGET_BYTES, MAX_ACTIVE_GLOBAL, MAX_ACTIVE_PER_CONNECTION,
        MAX_FRAME_PAYLOAD_BYTES, MAX_PENDING_BUDGET_BYTES, MAX_PENDING_GLOBAL,
        MAX_PENDING_PER_CONNECTION, MAX_TIMEOUT, PENDING_RESERVATION_BYTES,
        QUIC_STREAM_INTERNAL_CODE, QUIC_STREAM_OVERLOADED_CODE, QUIC_STREAM_PROTOCOL_CODE,
        QUIC_STREAM_TIMEOUT_CODE,
    };
    use crate::publication::PublishedRevision;
    use crate::publication::PublisherPolicy;
    use crate::server::InternalMetadata;
    use crate::Server;
    use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
    use distd_core::chunk_storage::ChunkStorage;
    use distd_core::chunks::{ChunkAlgorithm, ChunkInfo};
    use distd_core::hash::Hash;
    use distd_core::item::{Format as ItemFormat, Item, Manifest};
    use distd_core::metadata::Item as ItemMetadata;
    use distd_core::metadata::Server as ServerMetadata;
    use distd_core::possession::Bitfield;
    use distd_core::proto::{
        sync_message::Msg, ClientKeepAlive, ClientRegister, ManifestRequest, PayloadCompression,
        PossessionBitfield, ServerMetadata as ProtoServerMetadata, SyncMessage,
    };
    use distd_core::signature::{
        PublisherPublicKey, PublisherReleaseEnvelope, PublisherReleaseSequence,
        PublisherReleaseStatement, PublisherSignature,
    };
    use distd_core::transport::{
        decode_wire_rejection, encode_wire_preface, TransportOp, WireRejection,
        WireRejectionStatus, WireVersionDeclaration,
    };
    use distd_core::utils::frame::{
        decode_length_delimited, encode_length_delimited, read_length_delimited_async,
        write_current_wire_preface_async, write_length_delimited_async, write_optional_uuid_async,
        write_transport_op_async,
    };
    use distd_core::utils::serde::BitcodeSerializable;
    use distd_core::version::Version;
    use prost::Message as _;
    use quinn::{ClientConfig, Endpoint};
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error as TlsError, SignatureScheme};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize as TestAtomicUsize, Ordering as TestOrdering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime};
    use tokio::time::sleep;
    use uuid::Uuid;

    const FETCH_BENCH_OPERATIONS: u32 = 5_000;
    const FETCH_BENCH_RESPONSE_FRAME_BYTES: usize = 217;
    const FETCH_BENCH_RESPONSE_FRAME_BLAKE3: &str =
        "9b66dc2d89048d154be69e8e43be058df685224c58234a4f1fd9b86320122bb5";

    #[derive(Debug)]
    struct AcceptAnyServerCertVerifier;

    impl ServerCertVerifier for AcceptAnyServerCertVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, TlsError> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, TlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, TlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ED25519,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
            ]
        }
    }

    async fn connect_client(addr: SocketAddr) -> (Endpoint, quinn::Connection) {
        connect_client_with_receive_windows(addr, 64 * 1024 * 1024, 32 * 1024 * 1024).await
    }

    async fn connect_client_with_receive_windows(
        addr: SocketAddr,
        connection_window: u32,
        stream_window: u32,
    ) -> (Endpoint, quinn::Connection) {
        let client_bind = if addr.is_ipv6() {
            "[::]:0".parse().unwrap()
        } else {
            "0.0.0.0:0".parse().unwrap()
        };
        let mut endpoint = Endpoint::client(client_bind).expect("client endpoint");
        let rustls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCertVerifier))
            .with_no_client_auth();
        let mut client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)
                .expect("quic client config"),
        ));

        let mut transport = quinn::TransportConfig::default();
        transport.receive_window(quinn::VarInt::from_u32(connection_window));
        transport.stream_receive_window(quinn::VarInt::from_u32(stream_window));
        client_config.transport_config(std::sync::Arc::new(transport));

        endpoint.set_default_client_config(client_config);

        for _ in 0..20 {
            match endpoint.connect(addr, "localhost") {
                Ok(connecting) => match connecting.await {
                    Ok(connection) => return (endpoint, connection),
                    Err(_) => sleep(Duration::from_millis(25)).await,
                },
                Err(_) => sleep(Duration::from_millis(25)).await,
            }
        }

        panic!("failed to connect QUIC client to {addr}");
    }

    async fn open_authed_stream(
        connection: &quinn::Connection,
        op: TransportOp,
        uuid: Option<Uuid>,
    ) -> (quinn::SendStream, quinn::RecvStream) {
        let (mut send, recv) = connection.open_bi().await.expect("open bi");
        write_current_wire_preface_async(&mut send)
            .await
            .expect("write wire preface");
        write_transport_op_async(&mut send, op)
            .await
            .expect("write transport op");
        write_optional_uuid_async(&mut send, uuid)
            .await
            .expect("write auth uuid");
        (send, recv)
    }

    async fn start_test_server_with_metadata(
        metadata: InternalMetadata,
    ) -> (
        tokio::task::JoinHandle<()>,
        SocketAddr,
        Server<HashMapStorage>,
    ) {
        let (handle, addr, server, _) =
            start_test_server_with_metadata_and_settings(metadata, QuicResourceSettings::default())
                .await;
        (handle, addr, server)
    }

    async fn start_test_server_with_metadata_and_settings(
        metadata: InternalMetadata,
        settings: QuicResourceSettings,
    ) -> (
        tokio::task::JoinHandle<()>,
        SocketAddr,
        Server<HashMapStorage>,
        QuicAdmission,
    ) {
        let server = Server::new_ephemeral_with_metadata(HashMapStorage::default(), metadata)
            .with_enrollment_token("integration-token")
            .with_publisher_policy(PublisherPolicy::explicitly_insecure_unsigned());
        start_test_server_instance(server, settings)
    }

    fn start_test_server_instance(
        server: Server<HashMapStorage>,
        settings: QuicResourceSettings,
    ) -> (
        tokio::task::JoinHandle<()>,
        SocketAddr,
        Server<HashMapStorage>,
        QuicAdmission,
    ) {
        let endpoint = Endpoint::server(
            make_server_config().expect("server config"),
            "127.0.0.1:0".parse().unwrap(),
        )
        .expect("server endpoint");
        let addr = endpoint.local_addr().expect("local addr");
        let task_server = server.clone();
        let admission = QuicAdmission::new(settings).expect("valid test admission");
        let task_admission = admission.clone();
        let probe_enabled = task_admission.post_auth_prefix_probe_enabled();
        let handle = tokio::spawn(async move {
            if probe_enabled {
                task_server
                    .serve_quic_endpoint_specialized::<true>(endpoint, task_admission)
                    .await
            } else {
                task_server
                    .serve_quic_endpoint(endpoint, task_admission)
                    .await
            }
            .expect("serve test QUIC endpoint");
        });

        (handle, addr, server, admission)
    }

    fn signed_release(
        item: &Item,
        seed: [u8; 32],
        sequence: u64,
    ) -> (PublisherReleaseSequence, PublisherReleaseEnvelope) {
        let pair = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        let sequence = PublisherReleaseSequence::new(sequence);
        let statement = PublisherReleaseStatement::from_item(item, sequence).unwrap();
        let envelope = PublisherReleaseEnvelope::from_parts(
            PublisherPublicKey::try_from(pair.public_key().as_ref()).unwrap(),
            PublisherSignature::try_from(pair.sign(statement.as_bytes()).as_ref()).unwrap(),
        );
        (sequence, envelope)
    }

    async fn start_test_server() -> (
        tokio::task::JoinHandle<()>,
        SocketAddr,
        Server<HashMapStorage>,
    ) {
        start_test_server_with_metadata(InternalMetadata::default()).await
    }

    fn fetch_benchmark_internal_metadata() -> InternalMetadata {
        let root = ChunkInfo {
            size: 4_096,
            hash: Hash::from_bytes([0x5a; 32]),
        };
        let item = Item {
            metadata: ItemMetadata {
                artifact_id: "quic-fetch-fixture".to_string(),
                name: "quic-fetch-fixture".to_string(),
                description: Some("deterministic authenticated Fetch fixture".to_string()),
                revision: 7,
                path: PathBuf::from("fixtures/quic-fetch.bin"),
                root,
                created: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
                updated: SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_123),
                created_by: "distd-quic-fetch-bench-v1".to_string(),
                format: ItemFormat::V1,
            },
            manifest: Manifest {
                artifact_id: "quic-fetch-fixture".to_string(),
                version: 7,
                root_hash: root.hash,
                total_size: root.size,
                chunk_count: 1,
                chunk_size: 256 * 1024,
                chunk_algorithm: ChunkAlgorithm::Fixed256K,
                entries: Vec::new(),
            },
            chunks: vec![root],
        };

        InternalMetadata {
            version: Version::from((0, 2, 0)),
            feeds: HashMap::new(),
            items: HashMap::from([(
                item.manifest.artifact_id.clone(),
                PublishedRevision::unsigned(item),
            )]),
            versions: HashMap::new(),
        }
    }

    fn fetch_benchmark_response() -> ProtoServerMetadata {
        let metadata = ServerMetadata::from(fetch_benchmark_internal_metadata());
        ProtoServerMetadata {
            serialized: metadata.to_bitcode().expect("serialize benchmark metadata"),
            uuid: None,
        }
    }

    fn fetch_benchmark_response_frame() -> Vec<u8> {
        encode_length_delimited(&fetch_benchmark_response()).expect("encode benchmark response")
    }

    fn assert_fetch_benchmark_response_identity(frame: &[u8]) {
        assert_eq!(frame.len(), FETCH_BENCH_RESPONSE_FRAME_BYTES);
        assert_eq!(
            blake3::hash(frame).to_hex().as_str(),
            FETCH_BENCH_RESPONSE_FRAME_BLAKE3
        );
    }

    async fn read_raw_length_delimited_frame(recv: &mut quinn::RecvStream) -> Vec<u8> {
        let mut length = [0_u8; 4];
        recv.read_exact(&mut length)
            .await
            .expect("read response frame length");
        let payload_len = u32::from_be_bytes(length) as usize;
        let mut frame = Vec::with_capacity(4 + payload_len);
        frame.extend_from_slice(&length);
        frame.resize(4 + payload_len, 0);
        recv.read_exact(&mut frame[4..])
            .await
            .expect("read response frame payload");
        frame
    }

    async fn fetch_raw_response(connection: &quinn::Connection, uuid: Uuid) -> Vec<u8> {
        let (mut send, mut recv) =
            open_authed_stream(connection, TransportOp::Fetch, Some(uuid)).await;
        write_length_delimited_async(&mut send, &ClientKeepAlive {})
            .await
            .expect("write fetch request");
        send.finish().expect("finish fetch request");
        let frame = read_raw_length_delimited_frame(&mut recv).await;
        let trailing = recv
            .read_to_end(0)
            .await
            .expect("Fetch response stream must finish after one frame");
        assert!(trailing.is_empty(), "Fetch response had trailing bytes");
        frame
    }

    async fn register_client_uuid(connection: &quinn::Connection) -> Uuid {
        let (mut send, mut recv) =
            open_authed_stream(connection, TransportOp::Register, None).await;
        write_length_delimited_async(
            &mut send,
            &ClientRegister {
                name: "test-client".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                uuid: None,
                enrollment_token: Some("integration-token".to_string()),
            },
        )
        .await
        .expect("write register request");
        send.finish().expect("finish register");
        let register =
            read_length_delimited_async::<_, distd_core::proto::ServerMetadata>(&mut recv)
                .await
                .expect("read register response");
        Uuid::from_slice(register.uuid.as_ref().expect("uuid present")).expect("valid uuid")
    }

    async fn assert_stream_reset(
        send: &mut quinn::SendStream,
        recv: &mut quinn::RecvStream,
        expected_code: u32,
    ) {
        let expected = quinn::VarInt::from_u32(expected_code);
        let stopped = tokio::time::timeout(Duration::from_secs(2), send.stopped())
            .await
            .expect("peer-visible stop deadline")
            .expect("server stopped request stream");
        assert_eq!(stopped, Some(expected));
        let mut byte = [0_u8; 1];
        match tokio::time::timeout(Duration::from_secs(2), recv.read(&mut byte)).await {
            Ok(Err(quinn::ReadError::Reset(code))) => assert_eq!(code, expected),
            other => {
                panic!("expected peer-visible response reset {expected_code:#x}, got {other:?}")
            }
        }
    }

    async fn assert_response_reset_after_complete_request(
        send: &mut quinn::SendStream,
        recv: &mut quinn::RecvStream,
        expected_code: u32,
    ) {
        let expected = quinn::VarInt::from_u32(expected_code);
        let stopped = tokio::time::timeout(Duration::from_secs(2), send.stopped())
            .await
            .expect("request completion/stop deadline")
            .expect("request stream status");
        assert!(
            stopped.is_none() || stopped == Some(expected),
            "a fully consumed request may report None, otherwise it must carry the reset code"
        );
        let mut byte = [0_u8; 1];
        match tokio::time::timeout(Duration::from_secs(2), recv.read(&mut byte)).await {
            Ok(Err(quinn::ReadError::Reset(code))) => assert_eq!(code, expected),
            other => {
                panic!("expected peer-visible response reset {expected_code:#x}, got {other:?}")
            }
        }
    }

    async fn wait_for_admission(
        admission: &QuicAdmission,
        predicate: impl Fn(super::AdmissionSnapshot) -> bool,
    ) -> super::AdmissionSnapshot {
        for _ in 0..500 {
            let snapshot = admission.snapshot();
            if predicate(snapshot) {
                return snapshot;
            }
            sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "admission state did not reach expected condition: {:?}",
            admission.snapshot()
        );
    }

    #[test]
    fn quic_resource_settings_validate_every_hard_boundary() {
        QuicResourceSettings::default()
            .validate()
            .expect("defaults must remain valid");

        let invalid_counts: [fn(&mut QuicResourceSettings); 12] = [
            |settings: &mut QuicResourceSettings| settings.pending_global = 0,
            |settings: &mut QuicResourceSettings| {
                settings.pending_global = MAX_PENDING_GLOBAL + 1;
            },
            |settings: &mut QuicResourceSettings| settings.pending_per_connection = 0,
            |settings: &mut QuicResourceSettings| {
                settings.pending_per_connection = MAX_PENDING_PER_CONNECTION + 1;
            },
            |settings: &mut QuicResourceSettings| settings.active_global = 0,
            |settings: &mut QuicResourceSettings| {
                settings.active_global = MAX_ACTIVE_GLOBAL + 1;
            },
            |settings: &mut QuicResourceSettings| settings.active_per_connection = 0,
            |settings: &mut QuicResourceSettings| {
                settings.active_per_connection = MAX_ACTIVE_PER_CONNECTION + 1;
            },
            |settings: &mut QuicResourceSettings| settings.pending_budget_bytes = 0,
            |settings: &mut QuicResourceSettings| {
                settings.pending_budget_bytes = MAX_PENDING_BUDGET_BYTES + 1;
            },
            |settings: &mut QuicResourceSettings| settings.active_budget_bytes = 0,
            |settings: &mut QuicResourceSettings| {
                settings.active_budget_bytes = MAX_ACTIVE_BUDGET_BYTES + 1;
            },
        ];
        for mutate in invalid_counts {
            let mut settings = QuicResourceSettings::default();
            mutate(&mut settings);
            assert!(settings.validate().is_err());
        }

        let mut settings = QuicResourceSettings::default();
        settings.pending_global = 3;
        settings.pending_per_connection = 4;
        assert!(settings.validate().is_err());

        let mut settings = QuicResourceSettings::default();
        settings.active_global = 1;
        settings.active_per_connection = 2;
        assert!(settings.validate().is_err());

        let mut settings = QuicResourceSettings::default();
        settings.pending_budget_bytes = settings.pending_global * PENDING_RESERVATION_BYTES - 1;
        assert!(settings.validate().is_err());

        let mut settings = QuicResourceSettings::default();
        settings.active_budget_bytes = settings.active_global * ACTIVE_RESERVATION_BYTES - 1;
        assert!(settings.validate().is_err());

        let invalid_durations: [fn(&mut QuicResourceSettings); 7] = [
            |settings: &mut QuicResourceSettings| settings.handshake_timeout = Duration::ZERO,
            |settings: &mut QuicResourceSettings| {
                settings.connection_idle_timeout = MAX_TIMEOUT + Duration::from_millis(1);
            },
            |settings: &mut QuicResourceSettings| settings.prelude_timeout = Duration::ZERO,
            |settings: &mut QuicResourceSettings| settings.uuid_timeout = Duration::ZERO,
            |settings: &mut QuicResourceSettings| settings.request_timeout = Duration::ZERO,
            |settings: &mut QuicResourceSettings| settings.write_timeout = Duration::ZERO,
            |settings: &mut QuicResourceSettings| settings.stream_lifetime = Duration::ZERO,
        ];
        for mutate in invalid_durations {
            let mut settings = QuicResourceSettings::default();
            mutate(&mut settings);
            assert!(settings.validate().is_err());
        }

        let mut settings = QuicResourceSettings::default();
        settings.stream_lifetime = settings.request_timeout - Duration::from_millis(1);
        assert!(settings.validate().is_err());

        let mut settings = QuicResourceSettings::default();
        settings.stream_lifetime = settings.handshake_timeout - Duration::from_millis(1);
        assert!(settings.validate().is_err());

        let mut settings = QuicResourceSettings::default();
        settings.stream_lifetime = settings.connection_idle_timeout - Duration::from_millis(1);
        assert!(settings.validate().is_err());
    }

    #[test]
    fn every_valid_count_ceiling_implies_its_fixed_byte_equation() {
        for pending_global in 1..=MAX_PENDING_GLOBAL {
            let required = pending_global
                .checked_mul(PENDING_RESERVATION_BYTES)
                .expect("pending reservation equation must not overflow");
            let settings = QuicResourceSettings {
                pending_global,
                pending_per_connection: pending_global.min(MAX_PENDING_PER_CONNECTION),
                pending_budget_bytes: required,
                ..QuicResourceSettings::default()
            };
            settings.validate().expect("valid pending count ceiling");
            assert_eq!(required, settings.pending_budget_bytes);

            let undersized = QuicResourceSettings {
                pending_budget_bytes: required - 1,
                ..settings
            };
            assert!(undersized.validate().is_err());
        }

        for active_global in 1..=MAX_ACTIVE_GLOBAL {
            let required = active_global
                .checked_mul(ACTIVE_RESERVATION_BYTES)
                .expect("active reservation equation must not overflow");
            let settings = QuicResourceSettings {
                active_global,
                active_per_connection: active_global.min(MAX_ACTIVE_PER_CONNECTION),
                active_budget_bytes: required,
                ..QuicResourceSettings::default()
            };
            settings.validate().expect("valid active count ceiling");
            assert_eq!(required, settings.active_budget_bytes);

            let undersized = QuicResourceSettings {
                active_budget_bytes: required - 1,
                ..settings
            };
            assert!(undersized.validate().is_err());
        }
    }

    #[test]
    fn concurrent_snapshots_keep_peaks_above_current_and_derive_exact_bytes() {
        const WORKERS: usize = 8;
        const ITERATIONS: usize = 5_000;

        let admission =
            QuicAdmission::new(QuicResourceSettings::default()).expect("valid default admission");
        let start = Arc::new(Barrier::new(WORKERS + 1));
        let remaining = Arc::new(TestAtomicUsize::new(WORKERS));
        let mut workers = Vec::with_capacity(WORKERS);

        for _ in 0..WORKERS {
            let worker_admission = admission.clone();
            let worker_start = start.clone();
            let worker_remaining = remaining.clone();
            workers.push(thread::spawn(move || {
                let connection = worker_admission.connection();
                worker_start.wait();
                for _ in 0..ITERATIONS {
                    if let Ok(pending) = worker_admission.try_pending(&connection) {
                        std::hint::spin_loop();
                        drop(pending);
                    }
                    if let Ok(active) = worker_admission.try_active(&connection) {
                        std::hint::spin_loop();
                        drop(active);
                    }
                }
                worker_remaining.fetch_sub(1, TestOrdering::Release);
            }));
        }

        start.wait();
        while remaining.load(TestOrdering::Acquire) != 0 {
            let snapshot = admission.snapshot();
            assert!(snapshot.pending_peak >= snapshot.pending);
            assert!(snapshot.active_peak >= snapshot.active);
            assert_eq!(
                snapshot.pending_bytes,
                snapshot.pending * PENDING_RESERVATION_BYTES
            );
            assert_eq!(
                snapshot.pending_bytes_peak,
                snapshot.pending_peak * PENDING_RESERVATION_BYTES
            );
            assert_eq!(
                snapshot.active_bytes,
                snapshot.active * ACTIVE_RESERVATION_BYTES
            );
            assert_eq!(
                snapshot.active_bytes_peak,
                snapshot.active_peak * ACTIVE_RESERVATION_BYTES
            );
            assert!(snapshot.pending_bytes_peak >= snapshot.pending_bytes);
            assert!(snapshot.active_bytes_peak >= snapshot.active_bytes);
            thread::yield_now();
        }

        for worker in workers {
            worker.join().expect("admission worker panicked");
        }
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.pending, 0);
        assert_eq!(snapshot.active, 0);
        assert!(snapshot.pending_peak > 0);
        assert!(snapshot.active_peak > 0);
        assert!(snapshot.pending_bytes_peak >= snapshot.pending_bytes);
        assert!(snapshot.active_bytes_peak >= snapshot.active_bytes);
    }

    #[test]
    fn connection_context_handles_share_local_admission_and_release_guards() {
        let settings = QuicResourceSettings {
            pending_global: 2,
            pending_per_connection: 1,
            active_global: 2,
            active_per_connection: 1,
            ..QuicResourceSettings::default()
        };
        let admission = QuicAdmission::new(settings).expect("valid context admission");
        let server = Server::new_ephemeral_with_metadata(
            HashMapStorage::default(),
            InternalMetadata::default(),
        )
        .with_publisher_policy(PublisherPolicy::explicitly_insecure_unsigned());
        let context = Arc::new(super::ConnectionContext::new(server, admission));
        let stream_a = Arc::clone(&context);
        let stream_b = Arc::clone(&context);
        assert!(Arc::ptr_eq(&stream_a, &stream_b));

        let pending = stream_a
            .global_admission
            .try_pending(&stream_a.local_admission)
            .expect("first stream takes the connection pending slot");
        assert!(
            stream_b
                .global_admission
                .try_pending(&stream_b.local_admission)
                .is_err(),
            "a second context handle must observe the same local pending ceiling"
        );
        drop(pending);

        let active = stream_b
            .global_admission
            .try_active(&stream_b.local_admission)
            .expect("first stream takes the connection active slot");
        assert!(
            stream_a
                .global_admission
                .try_active(&stream_a.local_admission)
                .is_err(),
            "a second context handle must observe the same local active ceiling"
        );
        drop(active);

        let released = context.global_admission.snapshot();
        assert_eq!(released.pending, 0);
        assert_eq!(released.pending_bytes, 0);
        assert_eq!(released.active, 0);
        assert_eq!(released.active_bytes, 0);
        assert_eq!(context.local_admission.pending_count.available_permits(), 1);
        assert_eq!(context.local_admission.active_count.available_permits(), 1);
        assert_eq!(
            context
                .global_admission
                .global_pending_count
                .available_permits(),
            2
        );
        assert_eq!(
            context
                .global_admission
                .global_active_count
                .available_permits(),
            2
        );
    }

    #[test]
    fn quic_resource_settings_lookup_rejects_empty_invalid_overflow_and_non_utf8() {
        let defaults = QuicResourceSettings::from_lookup(|_| Ok(None))
            .expect("missing settings use validated defaults");
        assert_eq!(defaults.pending_global, MAX_PENDING_GLOBAL);
        assert_eq!(defaults.active_global, MAX_ACTIVE_GLOBAL);

        let values = HashMap::from([
            (super::PENDING_GLOBAL_ENV, "8".to_string()),
            (super::PENDING_PER_CONNECTION_ENV, "2".to_string()),
            (super::PENDING_BUDGET_MIB_ENV, "1".to_string()),
            (super::ACTIVE_GLOBAL_ENV, "2".to_string()),
            (super::ACTIVE_PER_CONNECTION_ENV, "1".to_string()),
            (super::ACTIVE_BUDGET_MIB_ENV, "128".to_string()),
            (super::HANDSHAKE_TIMEOUT_MS_ENV, "1000".to_string()),
            (super::CONNECTION_IDLE_TIMEOUT_MS_ENV, "2000".to_string()),
            (super::PRELUDE_TIMEOUT_MS_ENV, "500".to_string()),
            (super::UUID_TIMEOUT_MS_ENV, "500".to_string()),
            (super::REQUEST_TIMEOUT_MS_ENV, "1000".to_string()),
            (super::WRITE_TIMEOUT_MS_ENV, "1000".to_string()),
            (super::STREAM_LIFETIME_MS_ENV, "2000".to_string()),
        ]);
        let parsed = QuicResourceSettings::from_lookup(|name| Ok(values.get(name).cloned()))
            .expect("complete custom settings");
        assert_eq!(parsed.pending_global, 8);
        assert_eq!(parsed.pending_budget_bytes, 1024 * 1024);
        assert_eq!(parsed.active_global, 2);
        assert_eq!(parsed.active_budget_bytes, 128 * 1024 * 1024);
        assert_eq!(parsed.stream_lifetime, Duration::from_secs(2));

        for value in ["", "not-a-number", "18446744073709551616"] {
            let error = QuicResourceSettings::from_lookup(|name| {
                Ok((name == super::PENDING_GLOBAL_ENV).then(|| value.to_string()))
            })
            .expect_err("invalid setting");
            assert!(error.contains(super::PENDING_GLOBAL_ENV));
            if !value.is_empty() {
                assert!(!error.contains(value), "diagnostic must not echo input");
            }
        }

        let error = QuicResourceSettings::from_lookup(|name| {
            if name == super::PENDING_GLOBAL_ENV {
                Err(format!("{name} must contain valid UTF-8"))
            } else {
                Ok(None)
            }
        })
        .expect_err("non-UTF-8 setting");
        assert_eq!(
            error,
            format!("{} must contain valid UTF-8", super::PENDING_GLOBAL_ENV)
        );

        let overflow = usize::MAX.to_string();
        let error = QuicResourceSettings::from_lookup(|name| {
            Ok((name == super::PENDING_BUDGET_MIB_ENV).then(|| overflow.clone()))
        })
        .expect_err("MiB conversion overflow");
        assert!(error.contains(super::PENDING_BUDGET_MIB_ENV));
        assert!(!error.contains(&overflow));
    }

    #[test]
    fn admission_saturation_rolls_back_transactionally_and_releases_counters() {
        let settings = QuicResourceSettings {
            pending_global: 1,
            pending_per_connection: 1,
            active_global: 1,
            active_per_connection: 1,
            ..QuicResourceSettings::default()
        };
        let admission = QuicAdmission::new(settings).expect("valid admission");
        let connection_a = admission.connection();
        let connection_b = admission.connection();

        let handshake = admission.try_handshake().expect("first handshake");
        assert!(admission.try_pending(&connection_a).is_err());
        assert_eq!(connection_a.pending_count.available_permits(), 1);
        assert_eq!(admission.snapshot().handshakes, 1);
        drop(handshake);

        let pending = admission.try_pending(&connection_a).expect("first pending");
        assert!(admission.try_pending(&connection_a).is_err());
        assert!(admission.try_pending(&connection_b).is_err());
        assert_eq!(connection_b.pending_count.available_permits(), 1);
        let pending_snapshot = admission.snapshot();
        assert_eq!(pending_snapshot.pending, 1);
        assert_eq!(pending_snapshot.pending_peak, 1);
        assert_eq!(pending_snapshot.pending_bytes, PENDING_RESERVATION_BYTES);
        assert_eq!(
            pending_snapshot.pending_bytes_peak,
            PENDING_RESERVATION_BYTES
        );

        let active = admission.try_active(&connection_a).expect("first active");
        drop(pending);
        assert!(admission.try_active(&connection_a).is_err());
        assert!(admission.try_active(&connection_b).is_err());
        assert_eq!(connection_b.active_count.available_permits(), 1);
        let active_snapshot = admission.snapshot();
        assert_eq!(active_snapshot.active, 1);
        assert_eq!(active_snapshot.active_peak, 1);
        assert_eq!(active_snapshot.active_bytes, ACTIVE_RESERVATION_BYTES);
        assert_eq!(active_snapshot.active_bytes_peak, ACTIVE_RESERVATION_BYTES);
        drop(active);

        let final_snapshot = admission.snapshot();
        assert_eq!(final_snapshot.handshakes, 0);
        assert_eq!(final_snapshot.pending, 0);
        assert_eq!(final_snapshot.pending_bytes, 0);
        assert_eq!(final_snapshot.active, 0);
        assert_eq!(final_snapshot.active_bytes, 0);
        assert_eq!(final_snapshot.post_auth_prefix_reads, 0);
        assert!(!admission.post_auth_prefix_probe_enabled());
    }

    #[tokio::test]
    async fn admitted_handshake_helper_times_out_reports_and_releases_shared_guard() {
        let settings = QuicResourceSettings {
            handshake_timeout: Duration::from_millis(25),
            ..QuicResourceSettings::default()
        };
        let admission = QuicAdmission::new(settings.clone()).expect("valid admission");
        let handshake = admission.try_handshake().expect("acquire handshake guard");
        let deadline = tokio::time::Instant::now() + settings.handshake_timeout;

        let result = super::await_admitted_handshake(
            std::future::pending::<Result<(), ()>>(),
            handshake,
            deadline,
            &admission,
        )
        .await;
        assert!(matches!(
            result,
            Err(super::StreamFailure {
                phase: super::QuicPhase::Handshake,
                kind: super::StreamFailureKind::Timeout,
            })
        ));
        assert_eq!(admission.snapshot().handshakes, 0);
        assert_eq!(
            admission.global_pending_count.available_permits(),
            settings.pending_global
        );
        assert_eq!(
            admission.failure_events(),
            vec![(
                super::QuicPhase::Handshake,
                super::StreamFailureKind::Timeout
            )]
        );
    }

    #[tokio::test]
    async fn register_exact_limit_decodes_and_oversize_prefix_resets_before_payload() {
        let (server_task, addr, _server, admission) = start_test_server_with_metadata_and_settings(
            InternalMetadata::default(),
            QuicResourceSettings::default(),
        )
        .await;
        let (_endpoint, connection) = connect_client(addr).await;

        let request = (0..64)
            .find_map(|overhead| {
                let name_len = super::REGISTER_FRAME_MAX_BYTES.checked_sub(overhead)?;
                let request = ClientRegister {
                    name: "x".repeat(name_len),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    uuid: None,
                    enrollment_token: Some("integration-token".to_string()),
                };
                (request.encoded_len() == super::REGISTER_FRAME_MAX_BYTES).then_some(request)
            })
            .expect("construct exact-limit valid Register protobuf");
        let (mut boundary_send, mut boundary_recv) =
            open_authed_stream(&connection, TransportOp::Register, None).await;
        write_length_delimited_async(&mut boundary_send, &request)
            .await
            .expect("write exact-limit Register");
        boundary_send.finish().expect("finish exact-limit Register");
        read_length_delimited_async::<_, ProtoServerMetadata>(&mut boundary_recv)
            .await
            .expect("exact-limit Register reaches decode and response");

        for declared in [super::REGISTER_FRAME_MAX_BYTES + 1, MAX_FRAME_PAYLOAD_BYTES] {
            let (mut send, mut recv) =
                open_authed_stream(&connection, TransportOp::Register, None).await;
            send.write_all(&(declared as u32).to_be_bytes())
                .await
                .expect("write oversized Register prefix");
            assert_stream_reset(&mut send, &mut recv, QUIC_STREAM_PROTOCOL_CODE).await;
        }

        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;
        assert!(admission.snapshot().pending_bytes_peak <= MAX_PENDING_BUDGET_BYTES);
        server_task.abort();
    }

    #[tokio::test]
    async fn invalid_token_uuid_and_malformed_frame_release_every_reservation() {
        let (server_task, addr, _server, admission) = start_test_server_with_metadata_and_settings(
            InternalMetadata::default(),
            QuicResourceSettings::default(),
        )
        .await;
        let (_endpoint, connection) = connect_client(addr).await;

        let (mut token_send, mut token_recv) =
            open_authed_stream(&connection, TransportOp::Register, None).await;
        write_length_delimited_async(
            &mut token_send,
            &ClientRegister {
                name: "invalid-token".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                uuid: None,
                enrollment_token: Some("wrong-token".to_string()),
            },
        )
        .await
        .expect("write invalid-token Register");
        token_send.finish().expect("finish invalid-token Register");
        assert_response_reset_after_complete_request(
            &mut token_send,
            &mut token_recv,
            QUIC_STREAM_INTERNAL_CODE,
        )
        .await;
        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;

        let (mut uuid_send, mut uuid_recv) =
            open_authed_stream(&connection, TransportOp::Fetch, Some(Uuid::new_v4())).await;
        assert_stream_reset(&mut uuid_send, &mut uuid_recv, QUIC_STREAM_PROTOCOL_CODE).await;
        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;

        let (mut malformed_send, mut malformed_recv) =
            open_authed_stream(&connection, TransportOp::Register, None).await;
        malformed_send
            .write_all(&1_u32.to_be_bytes())
            .await
            .expect("write malformed length");
        malformed_send
            .write_all(&[0x80])
            .await
            .expect("write malformed protobuf");
        malformed_send.finish().expect("finish malformed frame");
        assert_response_reset_after_complete_request(
            &mut malformed_send,
            &mut malformed_recv,
            QUIC_STREAM_PROTOCOL_CODE,
        )
        .await;
        let released = wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;
        assert_eq!(released.pending_bytes, 0);
        assert_eq!(released.active_bytes, 0);
        server_task.abort();
    }

    #[tokio::test]
    async fn production_loop_enforces_per_connection_and_global_pending_limits() {
        let settings = QuicResourceSettings {
            pending_global: 2,
            pending_per_connection: 1,
            handshake_timeout: Duration::from_secs(5),
            connection_idle_timeout: Duration::from_secs(5),
            prelude_timeout: Duration::from_secs(5),
            uuid_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            stream_lifetime: Duration::from_secs(5),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, _server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;
        let (_endpoint_a, connection_a) = connect_client(addr).await;
        let (_endpoint_b, connection_b) = connect_client(addr).await;
        let (_endpoint_c, connection_c) = connect_client(addr).await;

        let (mut hold_a_send, mut hold_a_recv) =
            connection_a.open_bi().await.expect("open first pending");
        write_current_wire_preface_async(&mut hold_a_send)
            .await
            .expect("write first pending preface");
        write_transport_op_async(&mut hold_a_send, TransportOp::Register)
            .await
            .expect("start first pending");
        wait_for_admission(&admission, |snapshot| snapshot.pending == 1).await;

        let (mut same_connection_send, mut same_connection_recv) = connection_a
            .open_bi()
            .await
            .expect("open per-connection excess");
        write_current_wire_preface_async(&mut same_connection_send)
            .await
            .expect("write per-connection preface");
        write_transport_op_async(&mut same_connection_send, TransportOp::Register)
            .await
            .expect("start per-connection excess");
        assert_stream_reset(
            &mut same_connection_send,
            &mut same_connection_recv,
            QUIC_STREAM_OVERLOADED_CODE,
        )
        .await;

        let (mut hold_b_send, mut hold_b_recv) =
            connection_b.open_bi().await.expect("open second pending");
        write_current_wire_preface_async(&mut hold_b_send)
            .await
            .expect("write second pending preface");
        write_transport_op_async(&mut hold_b_send, TransportOp::Register)
            .await
            .expect("start second pending");
        wait_for_admission(&admission, |snapshot| snapshot.pending == 2).await;

        let (mut global_excess_send, mut global_excess_recv) =
            connection_c.open_bi().await.expect("open global excess");
        write_current_wire_preface_async(&mut global_excess_send)
            .await
            .expect("write global excess preface");
        write_transport_op_async(&mut global_excess_send, TransportOp::Register)
            .await
            .expect("start global excess");
        assert_stream_reset(
            &mut global_excess_send,
            &mut global_excess_recv,
            QUIC_STREAM_OVERLOADED_CODE,
        )
        .await;

        let snapshot = admission.snapshot();
        assert_eq!(snapshot.pending_peak, 2);
        assert_eq!(snapshot.pending_bytes_peak, 2 * PENDING_RESERVATION_BYTES);
        let close_code = quinn::VarInt::from_u32(0);
        let _ = hold_a_send.reset(close_code);
        let _ = hold_a_recv.stop(close_code);
        let _ = hold_b_send.reset(close_code);
        let _ = hold_b_recv.stop(close_code);
        wait_for_admission(&admission, |snapshot| snapshot.pending == 0).await;
        server_task.abort();
    }

    #[tokio::test]
    async fn production_loop_enforces_exact_default_pending_caps_across_connections() {
        let settings = QuicResourceSettings {
            connection_idle_timeout: Duration::from_secs(30),
            prelude_timeout: Duration::from_secs(30),
            uuid_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            stream_lifetime: Duration::from_secs(30),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, _server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;

        // Establish the excess connection before filling the shared pending pool. Handshakes
        // deliberately share the global pre-auth count, so a saturated server must refuse new
        // handshakes as well as new streams.
        let mut clients = Vec::new();
        for _ in 0..(MAX_PENDING_GLOBAL / MAX_PENDING_PER_CONNECTION + 1) {
            clients.push(connect_client(addr).await);
        }

        let mut held = Vec::with_capacity(MAX_PENDING_GLOBAL);
        for (_, connection) in clients
            .iter()
            .take(MAX_PENDING_GLOBAL / MAX_PENDING_PER_CONNECTION)
        {
            for _ in 0..MAX_PENDING_PER_CONNECTION {
                let (mut send, recv) = connection.open_bi().await.expect("open pending stream");
                write_current_wire_preface_async(&mut send)
                    .await
                    .expect("write pending preface");
                write_transport_op_async(&mut send, TransportOp::Register)
                    .await
                    .expect("start pending Register");
                held.push((send, recv));
            }
        }
        let saturated = wait_for_admission(&admission, |snapshot| {
            snapshot.pending == MAX_PENDING_GLOBAL
        })
        .await;
        assert_eq!(saturated.pending_peak, MAX_PENDING_GLOBAL);
        assert_eq!(saturated.pending_bytes, MAX_PENDING_BUDGET_BYTES);
        assert_eq!(saturated.pending_bytes_peak, MAX_PENDING_BUDGET_BYTES);

        let (_, first_connection) = &clients[0];
        let (mut per_connection_send, mut per_connection_recv) = first_connection
            .open_bi()
            .await
            .expect("open per-connection excess");
        write_current_wire_preface_async(&mut per_connection_send)
            .await
            .expect("write per-connection preface");
        write_transport_op_async(&mut per_connection_send, TransportOp::Register)
            .await
            .expect("start per-connection excess");
        assert_stream_reset(
            &mut per_connection_send,
            &mut per_connection_recv,
            QUIC_STREAM_OVERLOADED_CODE,
        )
        .await;

        let (_, excess_connection) = clients.last().expect("excess connection established");
        let (mut global_send, mut global_recv) = excess_connection
            .open_bi()
            .await
            .expect("open global excess");
        write_current_wire_preface_async(&mut global_send)
            .await
            .expect("write global preface");
        write_transport_op_async(&mut global_send, TransportOp::Register)
            .await
            .expect("start global excess");
        assert_stream_reset(
            &mut global_send,
            &mut global_recv,
            QUIC_STREAM_OVERLOADED_CODE,
        )
        .await;

        let mut oversized = held.drain(..32).collect::<Vec<_>>();
        for (send, _) in &mut oversized {
            write_optional_uuid_async(send, None)
                .await
                .expect("complete Register prelude");
            send.write_all(&(MAX_FRAME_PAYLOAD_BYTES as u32).to_be_bytes())
                .await
                .expect("write 32 MiB Register declaration");
        }
        for (mut send, mut recv) in oversized {
            assert_stream_reset(&mut send, &mut recv, QUIC_STREAM_PROTOCOL_CODE).await;
        }
        assert!(admission.snapshot().pending <= MAX_PENDING_GLOBAL);
        assert!(admission.snapshot().pending_bytes <= MAX_PENDING_BUDGET_BYTES);

        let close_code = quinn::VarInt::from_u32(0);
        for (mut send, mut recv) in held {
            let _ = send.reset(close_code);
            let _ = recv.stop(close_code);
        }
        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.pending_bytes == 0
        })
        .await;
        server_task.abort();
    }

    #[tokio::test]
    async fn saturated_active_budget_resets_before_post_auth_prefix_read() {
        let settings = QuicResourceSettings {
            active_global: 1,
            active_per_connection: 1,
            post_auth_prefix_probe: true,
            handshake_timeout: Duration::from_secs(5),
            connection_idle_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            stream_lifetime: Duration::from_secs(5),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, _server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;
        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;

        let (mut holder_send, mut holder_recv) =
            open_authed_stream(&connection, TransportOp::Fetch, Some(uuid)).await;
        let held = wait_for_admission(&admission, |snapshot| snapshot.active == 1).await;
        assert_eq!(held.post_auth_prefix_reads, 1);

        let (mut rejected_send, _rejected_recv) =
            open_authed_stream(&connection, TransportOp::Fetch, Some(uuid)).await;
        rejected_send
            .write_all(&(MAX_FRAME_PAYLOAD_BYTES as u32).to_be_bytes())
            .await
            .expect("write declaration already buffered by transport");
        let stopped = tokio::time::timeout(Duration::from_secs(2), rejected_send.stopped())
            .await
            .expect("overload reset deadline")
            .expect("server stopped request stream");
        assert_eq!(
            stopped,
            Some(quinn::VarInt::from_u32(QUIC_STREAM_OVERLOADED_CODE))
        );
        let saturated = admission.snapshot();
        assert_eq!(saturated.active, 1);
        assert_eq!(saturated.post_auth_prefix_reads, 1);

        let close_code = quinn::VarInt::from_u32(0);
        let _ = holder_send.reset(close_code);
        let _ = holder_recv.stop(close_code);
        wait_for_admission(&admission, |snapshot| {
            snapshot.active == 0 && snapshot.pending == 0
        })
        .await;
        server_task.abort();
    }

    #[tokio::test]
    async fn production_loop_enforces_exact_active_caps_before_sync_request_read() {
        let settings = QuicResourceSettings {
            post_auth_prefix_probe: true,
            handshake_timeout: Duration::from_secs(5),
            connection_idle_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            stream_lifetime: Duration::from_secs(5),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, _server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;
        let (_endpoint_a, connection_a) = connect_client(addr).await;
        let (_endpoint_b, connection_b) = connect_client(addr).await;
        let (_endpoint_c, connection_c) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection_a).await;

        let mut held = Vec::with_capacity(MAX_ACTIVE_GLOBAL);
        for connection in [&connection_a, &connection_b] {
            for _ in 0..MAX_ACTIVE_PER_CONNECTION {
                held.push(open_authed_stream(connection, TransportOp::Sync, Some(uuid)).await);
            }
        }
        let saturated =
            wait_for_admission(&admission, |snapshot| snapshot.active == MAX_ACTIVE_GLOBAL).await;
        assert_eq!(saturated.active_peak, MAX_ACTIVE_GLOBAL);
        assert_eq!(saturated.active_bytes, MAX_ACTIVE_BUDGET_BYTES);
        assert_eq!(saturated.active_bytes_peak, MAX_ACTIVE_BUDGET_BYTES);
        assert_eq!(saturated.post_auth_prefix_reads, MAX_ACTIVE_GLOBAL);

        let (mut per_connection_send, mut per_connection_recv) =
            open_authed_stream(&connection_a, TransportOp::Sync, Some(uuid)).await;
        assert_stream_reset(
            &mut per_connection_send,
            &mut per_connection_recv,
            QUIC_STREAM_OVERLOADED_CODE,
        )
        .await;

        let (mut global_send, mut global_recv) =
            open_authed_stream(&connection_c, TransportOp::Sync, Some(uuid)).await;
        assert_stream_reset(
            &mut global_send,
            &mut global_recv,
            QUIC_STREAM_OVERLOADED_CODE,
        )
        .await;
        assert_eq!(
            admission.snapshot().post_auth_prefix_reads,
            MAX_ACTIVE_GLOBAL,
            "rejected Sync streams must not read a request length prefix"
        );

        server_task.abort();
        let _ = server_task.await;
        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0 && snapshot.active_bytes == 0
        })
        .await;
        drop(held);
    }

    #[tokio::test]
    async fn absolute_stream_lifetime_resets_trickled_request_and_releases_guards() {
        let settings = QuicResourceSettings {
            handshake_timeout: Duration::from_millis(300),
            connection_idle_timeout: Duration::from_millis(300),
            prelude_timeout: Duration::from_millis(300),
            uuid_timeout: Duration::from_millis(300),
            request_timeout: Duration::from_millis(300),
            write_timeout: Duration::from_millis(300),
            stream_lifetime: Duration::from_millis(400),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, _server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;
        let (_endpoint, connection) = connect_client(addr).await;
        let (mut send, _recv) = connection.open_bi().await.expect("open stream");
        write_current_wire_preface_async(&mut send)
            .await
            .expect("write stream preface");
        write_transport_op_async(&mut send, TransportOp::Register)
            .await
            .expect("write operation");
        sleep(Duration::from_millis(200)).await;
        write_optional_uuid_async(&mut send, None)
            .await
            .expect("write delayed UUID marker");

        let stopped = tokio::time::timeout(Duration::from_secs(1), send.stopped())
            .await
            .expect("absolute deadline")
            .expect("server stopped request stream");
        assert_eq!(
            stopped,
            Some(quinn::VarInt::from_u32(QUIC_STREAM_TIMEOUT_CODE))
        );
        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;
        server_task.abort();
    }

    #[tokio::test]
    async fn uuid_and_registration_phase_stalls_reset_both_directions_and_release() {
        let settings = QuicResourceSettings {
            handshake_timeout: Duration::from_millis(500),
            connection_idle_timeout: Duration::from_millis(500),
            prelude_timeout: Duration::from_millis(150),
            uuid_timeout: Duration::from_millis(150),
            request_timeout: Duration::from_millis(150),
            write_timeout: Duration::from_millis(150),
            stream_lifetime: Duration::from_secs(1),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, _server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;
        let (_endpoint, connection) = connect_client(addr).await;

        let (mut uuid_send, mut uuid_recv) = connection.open_bi().await.expect("open UUID stall");
        write_current_wire_preface_async(&mut uuid_send)
            .await
            .expect("write UUID-stall preface");
        write_transport_op_async(&mut uuid_send, TransportOp::Register)
            .await
            .expect("write op before UUID stall");
        assert_stream_reset(&mut uuid_send, &mut uuid_recv, QUIC_STREAM_TIMEOUT_CODE).await;
        wait_for_admission(&admission, |snapshot| snapshot.pending == 0).await;

        let (mut request_send, mut request_recv) =
            open_authed_stream(&connection, TransportOp::Register, None).await;
        assert_stream_reset(
            &mut request_send,
            &mut request_recv,
            QUIC_STREAM_TIMEOUT_CODE,
        )
        .await;
        wait_for_admission(&admission, |snapshot| snapshot.pending == 0).await;
        server_task.abort();
    }

    #[tokio::test]
    async fn production_prelude_reader_helper_times_out_and_reports_phase_only() {
        let settings = QuicResourceSettings {
            handshake_timeout: Duration::from_millis(500),
            connection_idle_timeout: Duration::from_millis(500),
            prelude_timeout: Duration::from_millis(100),
            uuid_timeout: Duration::from_millis(500),
            request_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_millis(500),
            stream_lifetime: Duration::from_millis(500),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, _server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;
        let (_endpoint, connection) = connect_client(addr).await;
        let (mut send, mut recv) = connection.open_bi().await.expect("open prelude stall");
        send.write_all(b"D").await.expect("start partial prelude");
        assert_stream_reset(&mut send, &mut recv, QUIC_STREAM_TIMEOUT_CODE).await;
        wait_for_admission(&admission, |snapshot| snapshot.pending == 0).await;
        assert_eq!(
            admission.failure_events(),
            vec![(super::QuicPhase::Prelude, super::StreamFailureKind::Timeout)],
            "failure telemetry retains only the credential-free phase/reason pair"
        );
        server_task.abort();
    }

    #[tokio::test]
    async fn incompatible_prefaces_are_bounded_before_operation_or_durable_state_access() {
        let directory =
            std::env::temp_dir().join(format!("distd-wire-compatibility-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let server = Server::new_ephemeral_with_metadata(HashMapStorage::default(), metadata)
            .with_enrollment_token("integration-token")
            .with_publisher_policy(PublisherPolicy::explicitly_insecure_unsigned())
            .with_metadata_store(store);
        let before_metadata = server.metadata_snapshot().await;
        let before_durable = std::fs::read(&paths.current).unwrap();
        let settings = QuicResourceSettings {
            post_auth_prefix_probe: true,
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, server, admission) = start_test_server_instance(server, settings);
        let (_endpoint, connection) = connect_client(addr).await;

        let cases = [
            (
                encode_wire_preface(WireVersionDeclaration {
                    preface_format: 2,
                    ..WireVersionDeclaration::CURRENT
                })
                .to_vec(),
                WireRejectionStatus::UnsupportedPrefaceVersion,
            ),
            (
                encode_wire_preface(WireVersionDeclaration {
                    operation_version: 2,
                    ..WireVersionDeclaration::CURRENT
                })
                .to_vec(),
                WireRejectionStatus::UnsupportedOperationVersion,
            ),
            (
                encode_wire_preface(WireVersionDeclaration {
                    protobuf_version: 2,
                    ..WireVersionDeclaration::CURRENT
                })
                .to_vec(),
                WireRejectionStatus::UnsupportedProtobufVersion,
            ),
            (
                encode_wire_preface(WireVersionDeclaration {
                    operation_version: 2,
                    protobuf_version: 2,
                    ..WireVersionDeclaration::CURRENT
                })
                .to_vec(),
                WireRejectionStatus::UnsupportedVersionPair,
            ),
            (
                {
                    let mut malformed =
                        encode_wire_preface(WireVersionDeclaration::CURRENT).to_vec();
                    malformed[0] ^= 1;
                    malformed
                },
                WireRejectionStatus::MalformedPreface,
            ),
        ];

        for (preface, expected_status) in cases {
            let (mut send, mut recv) = connection
                .open_bi()
                .await
                .expect("open incompatible stream");
            let mut request = preface;
            request.extend_from_slice(&[u8::from(TransportOp::Register), 0, 0, 0, 0, 0]);
            send.write_all(&request)
                .await
                .expect("write incompatible request");
            let _ = send.finish();
            let response = recv
                .read_to_end(64)
                .await
                .expect("read bounded compatibility rejection");
            assert_eq!(response.len(), distd_core::transport::WIRE_REJECTION_BYTES);
            let encoded: [u8; distd_core::transport::WIRE_REJECTION_BYTES] =
                response.try_into().unwrap();
            assert_eq!(
                decode_wire_rejection(&encoded).unwrap(),
                WireRejection::current(expected_status)
            );
        }

        let (mut send, mut recv) = connection.open_bi().await.expect("open truncated stream");
        send.write_all(&encode_wire_preface(WireVersionDeclaration::CURRENT)[..7])
            .await
            .expect("write truncated preface");
        send.finish().expect("finish truncated preface");
        let response = recv
            .read_to_end(64)
            .await
            .expect("read truncated-preface rejection");
        let encoded: [u8; distd_core::transport::WIRE_REJECTION_BYTES] =
            response.try_into().unwrap();
        assert_eq!(
            decode_wire_rejection(&encoded).unwrap(),
            WireRejection::current(WireRejectionStatus::MalformedPreface)
        );

        wait_for_admission(&admission, |snapshot| snapshot.pending == 0).await;
        let snapshot = admission.snapshot();
        assert_eq!(snapshot.active, 0);
        assert_eq!(snapshot.post_auth_prefix_reads, 0);
        assert_eq!(server.storage().size(), 0);
        assert_eq!(server.metadata_snapshot().await, before_metadata);
        assert_eq!(std::fs::read(&paths.current).unwrap(), before_durable);
        server_task.abort();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn connection_idle_and_sync_possession_stalls_timeout_and_release() {
        let settings = QuicResourceSettings {
            handshake_timeout: Duration::from_millis(500),
            connection_idle_timeout: Duration::from_millis(100),
            prelude_timeout: Duration::from_millis(200),
            uuid_timeout: Duration::from_millis(200),
            request_timeout: Duration::from_millis(200),
            write_timeout: Duration::from_millis(200),
            stream_lifetime: Duration::from_secs(1),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;

        let (_idle_endpoint, idle_connection) = connect_client(addr).await;
        let idle_error = tokio::time::timeout(Duration::from_secs(1), idle_connection.closed())
            .await
            .expect("idle connection deadline");
        match idle_error {
            quinn::ConnectionError::ApplicationClosed(close) => assert_eq!(
                close.error_code,
                quinn::VarInt::from_u32(QUIC_STREAM_TIMEOUT_CODE)
            ),
            other => panic!("expected application idle close, got {other:?}"),
        }

        server
            .publish_item(
                "possession-stall".to_string(),
                "possession-stall".into(),
                None,
                axum::body::Bytes::from_static(b"possession timeout fixture"),
                distd_core::chunks::ChunkAlgorithm::default(),
            )
            .await
            .expect("publish possession fixture");
        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;
        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Sync, Some(uuid)).await;
        wait_for_admission(&admission, |snapshot| snapshot.active == 1).await;
        write_length_delimited_async(
            &mut send,
            &ManifestRequest {
                artifact_id: "possession-stall".to_string(),
                version: None,
            },
        )
        .await
        .expect("write manifest request");
        let manifest = read_length_delimited_async::<_, SyncMessage>(&mut recv)
            .await
            .expect("read manifest response before possession stall");
        assert!(matches!(manifest.msg, Some(Msg::ManifestResponse(_))));
        assert_stream_reset(&mut send, &mut recv, QUIC_STREAM_TIMEOUT_CODE).await;
        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;
        server_task.abort();
    }

    #[tokio::test]
    async fn production_response_write_wrapper_times_out_and_reports_phase_only() {
        let settings = QuicResourceSettings {
            write_timeout: Duration::from_millis(25),
            ..QuicResourceSettings::default()
        };
        let admission = QuicAdmission::new(settings.clone()).expect("valid admission");
        let absolute = tokio::time::Instant::now() + settings.stream_lifetime;
        let deadline = tokio::time::Instant::now() + settings.write_timeout;
        let failure = super::write_response_at(
            deadline,
            absolute,
            std::future::pending::<Result<(), std::io::Error>>(),
        )
        .await;
        let failure = failure.expect_err("response writer that makes no progress must time out");
        super::report_failure(&admission, failure);
        assert_eq!(
            admission.failure_events(),
            vec![(
                super::QuicPhase::ResponseWrite,
                super::StreamFailureKind::Timeout
            )],
            "failure telemetry retains only the credential-free phase/reason pair"
        );
    }

    #[tokio::test]
    async fn sync_response_backpressure_times_out_in_production_handler_and_releases() {
        const CONNECTION_RECEIVE_WINDOW: u32 = 16 * 1024;
        const STREAM_RECEIVE_WINDOW: u32 = 8 * 1024;
        const FIXTURE_BYTES: usize = 2 * 1024 * 1024;
        const WRITE_TIMEOUT: Duration = Duration::from_millis(200);

        let settings = QuicResourceSettings {
            handshake_timeout: Duration::from_secs(1),
            connection_idle_timeout: Duration::from_secs(2),
            prelude_timeout: Duration::from_secs(1),
            uuid_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            write_timeout: WRITE_TIMEOUT,
            stream_lifetime: Duration::from_secs(3),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;

        let mut state = 0xd1b5_4a32_d192_ed03_u64;
        let mut fixture = Vec::with_capacity(FIXTURE_BYTES);
        while fixture.len() < FIXTURE_BYTES {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            fixture.extend_from_slice(&state.to_le_bytes());
        }
        fixture.truncate(FIXTURE_BYTES);
        let item = server
            .publish_item(
                "write-backpressure".to_string(),
                "write-backpressure.jpg".into(),
                None,
                axum::body::Bytes::from(fixture),
                ChunkAlgorithm::fixed_aligned(
                    u32::try_from(FIXTURE_BYTES).expect("fixture length fits u32"),
                    4096,
                ),
            )
            .await
            .expect("publish deterministic incompressible fixture");
        assert_eq!(
            item.chunks.len(),
            1,
            "fixture must remain one retrievable stored root chunk"
        );
        assert_eq!(
            usize::try_from(item.chunks[0].size).expect("fixture chunk size fits usize"),
            FIXTURE_BYTES
        );
        let possession = PossessionBitfield {
            bitfield: Bitfield::empty(item.manifest.chunk_count).to_bytes(),
        };
        let expected_payload = server
            .sync_chunk_response_messages(&item, possession.clone())
            .await
            .expect("build the same missing-chunk response as the production handler");
        let response_data_bytes: usize = expected_payload
            .iter()
            .map(|message| match &message.msg {
                Some(Msg::ChunkData(chunk)) => chunk.data.len(),
                Some(Msg::BulkData(bulk)) => bulk.data.len(),
                other => panic!("unexpected fixture response: {other:?}"),
            })
            .sum();
        let response_wire_bytes: usize = expected_payload
            .iter()
            .map(|message| 4 + message.encoded_len())
            .sum();
        assert_eq!(response_data_bytes, FIXTURE_BYTES);
        assert!(
            response_wire_bytes > usize::try_from(CONNECTION_RECEIVE_WINDOW).unwrap(),
            "fixture response must exceed the negotiated connection receive window"
        );
        assert!(
            response_wire_bytes > usize::try_from(STREAM_RECEIVE_WINDOW).unwrap(),
            "fixture response must exceed the negotiated stream receive window"
        );

        let (_endpoint, connection) = connect_client_with_receive_windows(
            addr,
            CONNECTION_RECEIVE_WINDOW,
            STREAM_RECEIVE_WINDOW,
        )
        .await;
        let uuid = register_client_uuid(&connection).await;
        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Sync, Some(uuid)).await;
        write_length_delimited_async(
            &mut send,
            &ManifestRequest {
                artifact_id: "write-backpressure".to_string(),
                version: None,
            },
        )
        .await
        .expect("write backpressure manifest request");
        let manifest = read_length_delimited_async::<_, SyncMessage>(&mut recv)
            .await
            .expect("read manifest before withholding response credit");
        assert!(matches!(manifest.msg, Some(Msg::ManifestResponse(_))));
        write_length_delimited_async(&mut send, &possession)
            .await
            .expect("write missing-chunk possession");
        send.finish().expect("finish Sync request");

        sleep(WRITE_TIMEOUT + WRITE_TIMEOUT).await;
        let released = wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;
        assert_eq!(released.pending_bytes, 0);
        assert_eq!(released.active_bytes, 0);

        let expected_code = quinn::VarInt::from_u32(QUIC_STREAM_TIMEOUT_CODE);
        let (reset_code, drained_bytes) = tokio::time::timeout(Duration::from_secs(2), async {
            let mut drained_bytes = 0_usize;
            let mut buffer = [0_u8; 4096];
            loop {
                match recv.read(&mut buffer).await {
                    Ok(Some(read)) => drained_bytes += read,
                    Ok(None) => panic!("backpressured response ended with FIN instead of reset"),
                    Err(quinn::ReadError::Reset(code)) => break (code, drained_bytes),
                    Err(error) => panic!("unexpected backpressured response error: {error}"),
                }
            }
        })
        .await
        .expect("drain buffered response through application reset");
        assert_eq!(reset_code, expected_code);
        assert!(drained_bytes < response_wire_bytes);
        assert!(admission.failure_events().contains(&(
            super::QuicPhase::ResponseWrite,
            super::StreamFailureKind::Timeout,
        )));

        connection.close(quinn::VarInt::from_u32(0), b"backpressure test complete");
        server_task.abort();
    }

    #[tokio::test]
    async fn peer_connection_close_and_server_task_abort_release_all_guards() {
        let settings = QuicResourceSettings {
            handshake_timeout: Duration::from_secs(5),
            connection_idle_timeout: Duration::from_secs(5),
            prelude_timeout: Duration::from_secs(5),
            uuid_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            stream_lifetime: Duration::from_secs(5),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, _server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;

        let (_pending_endpoint, pending_connection) = connect_client(addr).await;
        let (mut pending_send, _pending_recv) = pending_connection
            .open_bi()
            .await
            .expect("open pending stream");
        write_current_wire_preface_async(&mut pending_send)
            .await
            .expect("write pending preface");
        write_transport_op_async(&mut pending_send, TransportOp::Register)
            .await
            .expect("start pending stream");
        wait_for_admission(&admission, |snapshot| snapshot.pending == 1).await;
        pending_connection.close(quinn::VarInt::from_u32(0), b"test peer close");
        wait_for_admission(&admission, |snapshot| snapshot.pending == 0).await;

        let (_active_endpoint, active_connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&active_connection).await;
        let (_active_send, _active_recv) =
            open_authed_stream(&active_connection, TransportOp::Fetch, Some(uuid)).await;
        wait_for_admission(&admission, |snapshot| snapshot.active == 1).await;
        server_task.abort();
        let _ = server_task.await;
        wait_for_admission(&admission, |snapshot| {
            snapshot.handshakes == 0
                && snapshot.pending == 0
                && snapshot.pending_bytes == 0
                && snapshot.active == 0
                && snapshot.active_bytes == 0
        })
        .await;
    }

    #[tokio::test]
    async fn request_read_and_handler_share_one_nonrenewed_phase_deadline() {
        let settings = QuicResourceSettings {
            post_auth_prefix_probe: true,
            handshake_timeout: Duration::from_secs(1),
            connection_idle_timeout: Duration::from_secs(1),
            prelude_timeout: Duration::from_secs(1),
            uuid_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_millis(400),
            write_timeout: Duration::from_secs(1),
            stream_lifetime: Duration::from_secs(2),
            ..QuicResourceSettings::default()
        };
        let (server_task, addr, server, admission) =
            start_test_server_with_metadata_and_settings(InternalMetadata::default(), settings)
                .await;
        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;
        let metadata_write = server.lock_metadata_for_test().await;

        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Fetch, Some(uuid)).await;
        wait_for_admission(&admission, |snapshot| {
            snapshot.active == 1 && snapshot.post_auth_prefix_reads >= 1
        })
        .await;
        sleep(Duration::from_millis(300)).await;
        write_length_delimited_async(&mut send, &ClientKeepAlive {})
            .await
            .expect("write request near phase deadline");
        send.finish().expect("finish near-deadline request");
        assert_response_reset_after_complete_request(
            &mut send,
            &mut recv,
            QUIC_STREAM_TIMEOUT_CODE,
        )
        .await;
        drop(metadata_write);
        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;
        server_task.abort();
    }

    #[test]
    fn quic_fetch_benchmark_fixture_identity_is_stable() {
        let metadata = fetch_benchmark_internal_metadata();
        assert_eq!(metadata.items.len(), 1, "fixture must remain nonempty");

        let frame = fetch_benchmark_response_frame();
        assert_fetch_benchmark_response_identity(&frame);
        let decoded = decode_length_delimited::<ProtoServerMetadata>(&frame)
            .expect("decode benchmark response frame");
        assert_eq!(decoded, fetch_benchmark_response());
        assert!(!decoded.serialized.is_empty());
        assert!(decoded.uuid.is_none());
    }

    #[tokio::test]
    async fn quic_fetch_benchmark_fixture_uses_shared_server_loop_and_finishes_stream() {
        let expected_frame = fetch_benchmark_response_frame();
        assert_fetch_benchmark_response_identity(&expected_frame);

        let (server_task, addr, server, admission) = start_test_server_with_metadata_and_settings(
            fetch_benchmark_internal_metadata(),
            QuicResourceSettings::default(),
        )
        .await;
        assert!(
            !admission.post_auth_prefix_probe_enabled(),
            "benchmark fixture must not execute post-auth telemetry RMWs"
        );
        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;
        assert_eq!(server.clients.read().await.len(), 1);
        assert_eq!(
            server.authenticate_client_uuid(Some(uuid)).await,
            Ok(uuid),
            "benchmark client must be enrolled before Fetch"
        );

        let actual_frame = fetch_raw_response(&connection, uuid).await;
        assert_eq!(actual_frame, expected_frame);
        let decoded = decode_length_delimited::<ProtoServerMetadata>(&actual_frame)
            .expect("decode real handler response");
        assert_eq!(decoded, fetch_benchmark_response());

        server_task.abort();
    }

    /// Manual microbenchmark for the real authenticated QUIC Fetch handler.
    ///
    /// Each process invocation performs one enrolled connection setup followed by exactly one
    /// timed batch. The external protocol builds and hashes one test executable, then invokes that
    /// exact executable for labeled warmups and counterbalanced runs. For example:
    ///
    /// `cargo test --profile release -p distd_server --no-run`
    ///
    /// `DISTD_QUIC_FETCH_BENCH_RUN_ID=warmup-1 target/release/deps/distd_server-<hash> quic::tests::manual_quic_authenticated_fetch_handler_5000 --ignored --exact --nocapture`
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "manual release-only QUIC Fetch handler benchmark"]
    async fn manual_quic_authenticated_fetch_handler_5000() {
        assert!(
            !cfg!(debug_assertions),
            "debug assertions are enabled; build the external test executable with --profile release"
        );
        let run_id =
            std::env::var("DISTD_QUIC_FETCH_BENCH_RUN_ID").unwrap_or_else(|_| "manual".to_string());
        assert!(
            !run_id.is_empty()
                && run_id.len() <= 64
                && run_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte)),
            "DISTD_QUIC_FETCH_BENCH_RUN_ID must be 1..=64 ASCII alphanumeric, '-', '_', or '.'"
        );

        let expected_frame = fetch_benchmark_response_frame();
        assert_fetch_benchmark_response_identity(&expected_frame);
        let response_blake3 = blake3::hash(&expected_frame).to_hex().to_string();
        let response_frame_bytes = expected_frame.len();

        let (server_task, addr, server, admission) = start_test_server_with_metadata_and_settings(
            fetch_benchmark_internal_metadata(),
            QuicResourceSettings::default(),
        )
        .await;
        assert!(
            !admission.post_auth_prefix_probe_enabled(),
            "manual benchmark must not execute post-auth telemetry RMWs"
        );
        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;
        assert_eq!(server.clients.read().await.len(), 1);
        server
            .authenticate_client_uuid(Some(uuid))
            .await
            .expect("benchmark client enrolled before timed batch");

        let mut per_stream_ns = Vec::with_capacity(FETCH_BENCH_OPERATIONS as usize);
        let started = Instant::now();
        for operation in 0..FETCH_BENCH_OPERATIONS {
            let stream_started = Instant::now();
            let actual_frame = fetch_raw_response(&connection, uuid).await;
            per_stream_ns.push(
                u64::try_from(stream_started.elapsed().as_nanos())
                    .expect("per-stream duration fits u64"),
            );
            assert_eq!(
                actual_frame, expected_frame,
                "Fetch response identity changed at operation {operation}"
            );
        }
        let elapsed_ns = started.elapsed().as_nanos();
        let ns_per_op = elapsed_ns / u128::from(FETCH_BENCH_OPERATIONS);

        println!(
            "DISTD_QUIC_FETCH_BENCH {{\"schema\":\"distd.quic.fetch.v1\",\"run_id\":\"{run_id}\",\"debug_assertions\":{},\"operations\":{FETCH_BENCH_OPERATIONS},\"elapsed_ns\":{elapsed_ns},\"ns_per_op\":{ns_per_op},\"per_stream_ns\":{per_stream_ns:?},\"response_frame_bytes\":{response_frame_bytes},\"response_blake3\":\"{response_blake3}\"}}",
            cfg!(debug_assertions)
        );

        server_task.abort();
    }

    #[tokio::test]
    async fn quic_register_and_fetch_round_trip() {
        let (server_task, addr, server) = start_test_server().await;
        server
            .publish_item(
                "artifact-a".to_string(),
                "artifact-a".into(),
                Some("test artifact".to_string()),
                axum::body::Bytes::from_static(b"hello over quic"),
                distd_core::chunks::ChunkAlgorithm::default(),
            )
            .await
            .expect("publish test item");

        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;

        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Fetch, Some(uuid)).await;
        write_length_delimited_async(&mut send, &ClientKeepAlive {})
            .await
            .expect("write fetch request");
        send.finish().expect("finish fetch");

        let fetch = read_length_delimited_async::<_, distd_core::proto::ServerMetadata>(&mut recv)
            .await
            .expect("read fetch response");
        let decoded: ServerMetadata =
            bitcode::deserialize(&fetch.serialized).expect("decode metadata");
        assert!(decoded.items.contains_key("artifact-a"));

        server_task.abort();
    }

    #[tokio::test]
    async fn quic_register_and_fetch_round_trip_over_ipv6_when_available() {
        let server = Server::new_ephemeral_with_metadata(
            HashMapStorage::default(),
            InternalMetadata::default(),
        )
        .with_enrollment_token("integration-token")
        .with_publisher_policy(PublisherPolicy::explicitly_insecure_unsigned());
        let item = server
            .publish_item(
                "ipv6-artifact".to_string(),
                "ipv6-artifact".into(),
                None,
                axum::body::Bytes::from_static(b"IPv6 Sync payload"),
                distd_core::chunks::ChunkAlgorithm::default(),
            )
            .await
            .expect("publish IPv6 fixture before endpoint starts");
        let endpoint = match Endpoint::server(
            make_server_config().expect("server config"),
            "[::1]:0".parse().unwrap(),
        ) {
            Ok(endpoint) => endpoint,
            Err(error) if error.kind() == std::io::ErrorKind::AddrNotAvailable => return,
            Err(error) => panic!("bind IPv6 loopback: {error}"),
        };
        let addr = endpoint.local_addr().expect("IPv6 server address");
        let admission =
            QuicAdmission::new(QuicResourceSettings::default()).expect("default admission");
        let task_server = server.clone();
        let server_task = tokio::spawn(async move {
            task_server
                .serve_quic_endpoint(endpoint, admission)
                .await
                .expect("serve IPv6 QUIC endpoint");
        });

        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;
        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Fetch, Some(uuid)).await;
        write_length_delimited_async(&mut send, &ClientKeepAlive {})
            .await
            .expect("write IPv6 Fetch request");
        send.finish().expect("finish IPv6 Fetch request");
        read_length_delimited_async::<_, ProtoServerMetadata>(&mut recv)
            .await
            .expect("read IPv6 Fetch response");

        let (mut sync_send, mut sync_recv) =
            open_authed_stream(&connection, TransportOp::Sync, Some(uuid)).await;
        write_length_delimited_async(
            &mut sync_send,
            &ManifestRequest {
                artifact_id: "ipv6-artifact".to_string(),
                version: None,
            },
        )
        .await
        .expect("write IPv6 manifest request");
        let manifest = read_length_delimited_async::<_, SyncMessage>(&mut sync_recv)
            .await
            .expect("read IPv6 manifest response");
        assert!(matches!(manifest.msg, Some(Msg::ManifestResponse(_))));
        write_length_delimited_async(
            &mut sync_send,
            &PossessionBitfield {
                bitfield: Bitfield::empty(item.manifest.chunk_count).to_bytes(),
            },
        )
        .await
        .expect("write IPv6 possession");
        sync_send.finish().expect("finish IPv6 Sync request");
        let mut saw_payload = false;
        loop {
            match read_length_delimited_async::<_, SyncMessage>(&mut sync_recv).await {
                Ok(SyncMessage {
                    msg: Some(Msg::ChunkData(_) | Msg::BulkData(_)),
                }) => saw_payload = true,
                Ok(other) => panic!("unexpected IPv6 Sync frame: {other:?}"),
                Err(distd_core::utils::frame::FrameError::IoError(error))
                    if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break;
                }
                Err(error) => panic!("IPv6 Sync failed: {error}"),
            }
        }
        assert!(saw_payload, "IPv6 Sync must deliver payload and FIN");
        server_task.abort();
    }

    #[tokio::test]
    async fn quic_sync_round_trip_returns_manifest_and_payload() {
        let (server_task, addr, server) = start_test_server().await;
        let item = server
            .publish_item(
                "artifact-a".to_string(),
                "artifact-a".into(),
                Some("test artifact".to_string()),
                axum::body::Bytes::from_static(b"hello over quic"),
                distd_core::chunks::ChunkAlgorithm::default(),
            )
            .await
            .expect("publish test item");

        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;

        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Sync, Some(uuid)).await;
        write_length_delimited_async(
            &mut send,
            &ManifestRequest {
                artifact_id: "artifact-a".to_string(),
                version: None,
            },
        )
        .await
        .expect("write manifest request");

        let manifest = read_length_delimited_async::<_, SyncMessage>(&mut recv)
            .await
            .expect("read manifest frame");
        let manifest = match manifest.msg {
            Some(Msg::ManifestResponse(manifest)) => manifest,
            other => panic!("unexpected manifest response: {other:?}"),
        };
        assert_eq!(manifest.artifact_id, "artifact-a");
        assert_eq!(manifest.chunk_count as usize, item.chunks.len());

        write_length_delimited_async(
            &mut send,
            &PossessionBitfield {
                bitfield: Bitfield::empty(item.manifest.chunk_count).to_bytes(),
            },
        )
        .await
        .expect("write possession bitfield");
        send.finish().expect("finish sync request");

        let mut saw_payload = false;
        loop {
            match read_length_delimited_async::<_, SyncMessage>(&mut recv).await {
                Ok(SyncMessage {
                    msg: Some(Msg::ChunkData(chunk)),
                }) => {
                    assert_eq!(chunk.compression, PayloadCompression::Unspecified as i32);
                    assert!(!chunk.data.is_empty());
                    saw_payload = true;
                }
                Ok(SyncMessage {
                    msg: Some(Msg::BulkData(bulk)),
                }) => {
                    assert_eq!(bulk.compression, PayloadCompression::Unspecified as i32);
                    assert!(!bulk.data.is_empty());
                    saw_payload = true;
                }
                Ok(other) => panic!("unexpected sync frame: {other:?}"),
                Err(distd_core::utils::frame::FrameError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break
                }
                Err(err) => panic!("unexpected sync error: {err}"),
            }
        }
        assert!(saw_payload, "expected at least one payload frame");

        server_task.abort();
    }

    #[tokio::test]
    async fn quic_sync_latest_and_exact_versions_return_their_signed_envelopes() {
        let storage = HashMapStorage::default();
        let algorithm = ChunkAlgorithm::Fixed256K;
        let first_bytes = axum::body::Bytes::from_static(b"signed quic revision zero");
        let second_bytes = axum::body::Bytes::from_static(b"signed quic revision one");
        let prepare = |bytes: axum::body::Bytes| {
            storage
                .prepare_revision(
                    "signed-quic".to_string(),
                    "signed-quic".to_string(),
                    "signed-quic.bin".into(),
                    0,
                    None,
                    bytes,
                    algorithm,
                )
                .unwrap()
        };
        let (first_sequence, first_envelope) =
            signed_release(&prepare(first_bytes.clone()), [71; 32], 1);
        let (second_sequence, second_envelope) =
            signed_release(&prepare(second_bytes.clone()), [73; 32], 2);
        let policy =
            PublisherPolicy::required([first_envelope.public_key(), second_envelope.public_key()]);
        let server = Server::new_ephemeral_with_metadata(storage, InternalMetadata::default())
            .with_enrollment_token("integration-token")
            .with_publisher_policy(policy);
        let first = server
            .publish_release_item_as(
                "signed-quic".to_string(),
                "signed-quic".to_string(),
                "signed-quic.bin".into(),
                None,
                first_bytes,
                algorithm,
                first_sequence,
                first_envelope,
            )
            .await
            .unwrap();
        let second = server
            .publish_release_item_as(
                "signed-quic".to_string(),
                "signed-quic".to_string(),
                "signed-quic.bin".into(),
                None,
                second_bytes,
                algorithm,
                second_sequence,
                second_envelope,
            )
            .await
            .unwrap();
        let (server_task, addr, _, _) =
            start_test_server_instance(server, QuicResourceSettings::default());
        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;

        for (version, expected_item, expected_sequence, expected_envelope) in [
            (None, &second, second_sequence, second_envelope),
            (Some(0), &first, first_sequence, first_envelope),
        ] {
            let (mut send, mut recv) =
                open_authed_stream(&connection, TransportOp::Sync, Some(uuid)).await;
            write_length_delimited_async(
                &mut send,
                &ManifestRequest {
                    artifact_id: "signed-quic".to_string(),
                    version,
                },
            )
            .await
            .unwrap();
            let response = read_length_delimited_async::<_, SyncMessage>(&mut recv)
                .await
                .unwrap();
            let response = match response.msg {
                Some(Msg::ManifestResponse(response)) => response,
                other => panic!("unexpected signed manifest response: {other:?}"),
            };
            assert_eq!(response.version, expected_item.manifest.version);
            assert_eq!(
                response.publisher_public_key,
                expected_envelope.public_key().as_bytes()
            );
            assert_eq!(
                response.publisher_signature,
                expected_envelope.signature().as_bytes()
            );
            assert_eq!(response.publisher_statement_version, Some(2));
            assert_eq!(
                response.publisher_release_sequence,
                Some(expected_sequence.get())
            );
            write_length_delimited_async(
                &mut send,
                &PossessionBitfield {
                    bitfield: Bitfield::full(expected_item.manifest.chunk_count).to_bytes(),
                },
            )
            .await
            .unwrap();
            send.finish().unwrap();
            assert!(matches!(
                read_length_delimited_async::<_, SyncMessage>(&mut recv).await,
                Err(distd_core::utils::frame::FrameError::IoError(error))
                    if error.kind() == std::io::ErrorKind::UnexpectedEof
            ));
        }
        server_task.abort();
    }

    #[tokio::test]
    async fn quic_fetch_rejects_unauthenticated_client() {
        let (server_task, addr, _server, admission) = start_test_server_with_metadata_and_settings(
            InternalMetadata::default(),
            QuicResourceSettings::default(),
        )
        .await;
        let (_endpoint, connection) = connect_client(addr).await;

        let (mut send, mut recv) = open_authed_stream(&connection, TransportOp::Fetch, None).await;
        write_length_delimited_async(&mut send, &ClientKeepAlive {})
            .await
            .expect("write fetch request");
        send.finish().expect("finish fetch");
        assert_response_reset_after_complete_request(
            &mut send,
            &mut recv,
            QUIC_STREAM_PROTOCOL_CODE,
        )
        .await;
        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;

        server_task.abort();
    }

    #[tokio::test]
    async fn quic_sync_rejects_invalid_possession_bitfield() {
        let (server_task, addr, server, admission) = start_test_server_with_metadata_and_settings(
            InternalMetadata::default(),
            QuicResourceSettings::default(),
        )
        .await;
        server
            .publish_item(
                "artifact-a".to_string(),
                "artifact-a".into(),
                Some("test artifact".to_string()),
                axum::body::Bytes::from_static(b"hello over quic"),
                distd_core::chunks::ChunkAlgorithm::default(),
            )
            .await
            .expect("publish test item");

        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;

        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Sync, Some(uuid)).await;
        write_length_delimited_async(
            &mut send,
            &ManifestRequest {
                artifact_id: "artifact-a".to_string(),
                version: None,
            },
        )
        .await
        .expect("write manifest request");

        let manifest = read_length_delimited_async::<_, SyncMessage>(&mut recv)
            .await
            .expect("read manifest frame");
        match manifest.msg {
            Some(Msg::ManifestResponse(_)) => {}
            other => panic!("unexpected manifest response: {other:?}"),
        }

        write_length_delimited_async(
            &mut send,
            &PossessionBitfield {
                bitfield: vec![0, 0, 0, 0],
            },
        )
        .await
        .expect("write malformed possession bitfield");
        send.finish().expect("finish sync request");
        assert_stream_reset(&mut send, &mut recv, QUIC_STREAM_PROTOCOL_CODE).await;
        wait_for_admission(&admission, |snapshot| {
            snapshot.pending == 0 && snapshot.active == 0
        })
        .await;

        server_task.abort();
    }
}
