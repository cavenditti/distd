use crate::{
    error::ServerRequest,
    persistence::{DurableTrustReceipt, ServerTrustRecord},
    publisher::{PublisherAuthenticityError, PublisherPolicy},
    server_url::CanonicalServerUrl,
};

use rustc_hash::FxHashSet as HashSet;
use std::{fmt::Debug, sync::Arc, time::Duration};
use uuid::Uuid;

use tokio::{sync::RwLock, time::Instant};

//use ring::agreement::PublicKey;

use distd_core::{
    chunk_storage::ReceivedChunk,
    chunks::{ChunkAlgorithm, ChunkInfo},
    hash::Hash,
    item::{FileEntry, Manifest},
    metadata::{Item as ItemMetadata, Server as ServerMetadata},
    proto::{self},
    signature::{
        PublisherKeyFingerprint, PublisherPublicKey, PublisherSignature, PublisherSignatureEnvelope,
    },
    version::VERSION,
};

mod quic;

use quic::{QuicSyncSession, QuicTransportClient};

#[derive(Clone, Debug)]
enum TransportClient {
    Quic(QuicTransportClient),
}

#[derive(Debug)]
enum SyncContinuation {
    Quic(QuicSyncSession),
    #[cfg(test)]
    Observed(ObservedSyncSession),
}

#[cfg(test)]
#[derive(Debug, Default)]
struct ObservedSyncContinuation {
    possession_sends: std::sync::atomic::AtomicUsize,
    aborts: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
#[derive(Debug)]
struct ObservedSyncSession {
    observer: Arc<ObservedSyncContinuation>,
    completed: bool,
}

#[cfg(test)]
impl ObservedSyncSession {
    fn new(observer: Arc<ObservedSyncContinuation>) -> Self {
        Self {
            observer,
            completed: false,
        }
    }
}

#[cfg(test)]
impl Drop for ObservedSyncSession {
    fn drop(&mut self) {
        if !self.completed {
            self.observer
                .aborts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

#[derive(Debug)]
struct PendingSync {
    manifest: PendingManifest,
    continuation: SyncContinuation,
}

#[derive(Debug)]
enum PendingManifest {
    Framed(Vec<u8>),
    #[cfg(test)]
    Decoded(proto::ManifestResponse),
}

impl PendingManifest {
    fn decode(self) -> Result<proto::ManifestResponse, PublisherAuthenticityError> {
        let message = match self {
            Self::Framed(frame) => {
                distd_core::utils::frame::decode_length_delimited::<proto::SyncMessage>(&frame)
                    .map_err(|_| PublisherAuthenticityError::MalformedAttestation)?
            }
            #[cfg(test)]
            Self::Decoded(response) => return Ok(response),
        };
        match message.msg {
            Some(proto::sync_message::Msg::ManifestResponse(response)) => Ok(response),
            _ => Err(PublisherAuthenticityError::MalformedAttestation),
        }
    }
}

#[derive(Debug)]
struct VerifiedSync {
    manifest: Manifest,
    chunks: Vec<ChunkInfo>,
    chunk_sizes: Vec<usize>,
    possession: proto::PossessionBitfield,
    publisher_fingerprint: Option<PublisherKeyFingerprint>,
    local_hashes: Vec<Hash>,
    continuation: SyncContinuation,
}

type VerifiedManifestParts = (
    Manifest,
    Vec<ChunkInfo>,
    Vec<usize>,
    proto::PossessionBitfield,
    Option<PublisherKeyFingerprint>,
    Vec<Hash>,
);

impl TransportClient {
    fn certificate_fingerprint(&self) -> [u8; 32] {
        match self {
            Self::Quic(client) => client.certificate_fingerprint(),
        }
    }

    async fn register(
        &self,
        request: distd_core::proto::ClientRegister,
    ) -> Result<distd_core::proto::ServerMetadata, ServerRequest> {
        match self {
            Self::Quic(client) => client.register(request).await,
        }
    }

    async fn fetch(
        &self,
        request: distd_core::proto::ClientKeepAlive,
    ) -> Result<distd_core::proto::ServerMetadata, ServerRequest> {
        match self {
            Self::Quic(client) => client.fetch(request).await,
        }
    }

    async fn set_client_uuid(&self, client_uuid: Uuid) {
        match self {
            Self::Quic(client) => client.set_client_uuid(client_uuid).await,
        }
    }

    async fn open_sync(
        &self,
        manifest_request: proto::ManifestRequest,
    ) -> Result<PendingSync, ServerRequest> {
        match self {
            Self::Quic(client) => {
                let (manifest, session) = client.sync(manifest_request).await?;
                Ok(PendingSync {
                    manifest: PendingManifest::Framed(manifest),
                    continuation: SyncContinuation::Quic(session),
                })
            }
        }
    }

    fn close(&self) {
        match self {
            Self::Quic(client) => client.close(),
        }
    }
}

impl SyncContinuation {
    #[allow(dead_code)]
    async fn send_possession_and_collect_chunks(
        self,
        possession: proto::PossessionBitfield,
        chunk_sizes: &[usize],
    ) -> Result<(Vec<Vec<u8>>, u64), ServerRequest> {
        let (received, wire_bytes) = self
            .send_possession_and_collect_chunks_shared(possession, chunk_sizes)
            .await?;
        Ok((
            received.into_iter().map(ReceivedChunk::into_vec).collect(),
            wire_bytes,
        ))
    }

    async fn send_possession_and_collect_chunks_shared(
        self,
        possession: proto::PossessionBitfield,
        chunk_sizes: &[usize],
    ) -> Result<(Vec<ReceivedChunk>, u64), ServerRequest> {
        match self {
            Self::Quic(session) => {
                session
                    .send_possession_and_collect_chunks_shared(possession, chunk_sizes)
                    .await
            }
            #[cfg(test)]
            Self::Observed(mut observed) => {
                observed
                    .observer
                    .possession_sends
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let bitfield = distd_core::possession::Bitfield::from_bytes(&possession.bitfield)
                    .ok_or(ServerRequest::UnexpectedMessage)?;
                if bitfield.chunk_count() as usize != chunk_sizes.len() || !bitfield.is_complete() {
                    return Err(ServerRequest::UnexpectedMessage);
                }
                observed.completed = true;
                Ok((Vec::new(), 0))
            }
        }
    }

    fn abort(self) {
        match self {
            Self::Quic(session) => session.abort(),
            #[cfg(test)]
            Self::Observed(mut observed) => {
                observed
                    .observer
                    .aborts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                observed.completed = true;
            }
        }
    }
}

impl VerifiedSync {
    async fn transfer(
        self,
    ) -> Result<(Manifest, Vec<ChunkInfo>, Vec<ReceivedChunk>, u64, Vec<Hash>), ServerRequest> {
        if let Some(fingerprint) = self.publisher_fingerprint {
            tracing::debug!(%fingerprint, "publisher manifest verified");
        } else {
            tracing::warn!("unsigned publisher manifest accepted by explicit insecure policy");
        }
        let (received, wire_bytes) = self
            .continuation
            .send_possession_and_collect_chunks_shared(self.possession, &self.chunk_sizes)
            .await?;
        Ok((
            self.manifest,
            self.chunks,
            received,
            wire_bytes,
            self.local_hashes,
        ))
    }
}

/// Shared server-related data to be kept behind an async lock
#[derive(Debug)]
struct SharedServer {
    /// global server metadata
    pub metadata: Arc<ServerMetadata>,

    /// last time metadata was fetched from server
    pub last_update: Instant,

    /// transport client for server requests
    pub transport: TransportClient,
}

/// Server representation used by clients
#[derive(Clone)]
pub struct Server {
    //pub connection: ..

    // server address
    //pub addr: SocketAddr,
    /// server url
    url: CanonicalServerUrl,

    /// BLAKE3 fingerprint of the authenticated server certificate DER.
    certificate_pin: [u8; 32],

    /// Publisher authority configured independently from the TLS trust root.
    publisher_policy: PublisherPolicy,

    /// Client Uuid assigned to client from server
    client_uuid: Option<Uuid>,

    /// Client name
    client_name: String,

    enrollment_token: Option<String>,

    /// Shared data
    shared: Arc<RwLock<SharedServer>>,

    /// Elapsed time between server fetches
    pub timeout: Duration,
}

impl Debug for Server {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Server")
            .field("url", &self.url)
            .field("certificate_pin", &self.certificate_pin)
            .field("publisher_policy", &self.publisher_policy)
            .field("client_uuid", &self.client_uuid)
            .field("client_name", &self.client_name)
            .field(
                "enrollment_token",
                &redacted_token(self.enrollment_token.as_deref()),
            )
            .field("shared", &self.shared)
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// A connected and registered server whose trust record is not yet authorized
/// for use by the client.
///
/// The pending value deliberately exposes no metadata or sync API. The caller
/// must durably install its observed certificate and assigned UUID before
/// consuming it with [`PendingServer::activate`].
pub struct PendingServer {
    inner: Option<PendingServerInner>,
}

struct PendingServerInner {
    url: CanonicalServerUrl,
    certificate_fingerprint: [u8; 32],
    publisher_policy: PublisherPolicy,
    client_uuid: Uuid,
    client_name: String,
    enrollment_token: Option<String>,
    transport: TransportClient,
    timeout: Duration,
}

impl Debug for PendingServer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut pending = formatter.debug_struct("PendingServer");
        if let Some(inner) = &self.inner {
            pending
                .field("url", &inner.url)
                .field("certificate_fingerprint", &inner.certificate_fingerprint)
                .field("publisher_policy", &inner.publisher_policy)
                .field("client_uuid", &inner.client_uuid)
                .field("client_name", &inner.client_name)
                .field(
                    "enrollment_token",
                    &redacted_token(inner.enrollment_token.as_deref()),
                )
                .field("timeout", &inner.timeout);
        } else {
            pending.field("state", &"consumed");
        }
        pending.finish_non_exhaustive()
    }
}

impl Drop for PendingServer {
    fn drop(&mut self) {
        if let Some(inner) = &self.inner {
            inner.transport.close();
        }
    }
}

impl PendingServer {
    /// Canonical authority under which the complete trust record must be stored.
    #[must_use]
    pub fn canonical_url(&self) -> &CanonicalServerUrl {
        &self.inner().url
    }

    /// Certificate fingerprint observed during the authenticated handshake.
    #[must_use]
    pub fn observed_pin(&self) -> [u8; 32] {
        self.inner().certificate_fingerprint
    }

    /// Non-nil UUID assigned (or confirmed) by the server during registration.
    #[must_use]
    pub fn client_uuid(&self) -> Uuid {
        self.inner().client_uuid
    }

    /// Fetch initial metadata and turn this pending enrollment into a usable server.
    ///
    /// Callers must durably store `canonical_url`, `observed_pin`, and
    /// `client_uuid` as one record before calling this method.
    pub(crate) async fn activate(
        mut self,
        receipt: DurableTrustReceipt,
    ) -> Result<Server, ServerRequest> {
        let inner = self
            .inner
            .take()
            .expect("pending server can only be activated once");
        if !receipt_matches_pending(
            &receipt,
            &inner.url,
            inner.certificate_fingerprint,
            inner.client_uuid,
        ) {
            inner.transport.close();
            return Err(ServerRequest::ClientIdentityChanged);
        }
        let metadata = match fetch_metadata(&inner.transport).await {
            Ok(metadata) => metadata,
            Err(error) => {
                inner.transport.close();
                return Err(error);
            }
        };

        Ok(Server {
            certificate_pin: inner.certificate_fingerprint,
            publisher_policy: inner.publisher_policy,
            url: inner.url,
            client_uuid: Some(inner.client_uuid),
            client_name: inner.client_name,
            enrollment_token: inner.enrollment_token,
            shared: Arc::new(RwLock::new(SharedServer {
                metadata: Arc::new(metadata),
                transport: inner.transport,
                last_update: Instant::now(),
            })),
            timeout: inner.timeout,
        })
    }

    fn inner(&self) -> &PendingServerInner {
        self.inner
            .as_ref()
            .expect("pending server has not been consumed")
    }
}

impl Server {
    /// Canonical immutable authority bound to this server's persisted identity.
    #[must_use]
    pub fn canonical_url(&self) -> &CanonicalServerUrl {
        &self.url
    }

    /// Immutable certificate pin bound to this server's persisted identity.
    #[must_use]
    pub fn certificate_pin(&self) -> [u8; 32] {
        self.certificate_pin
    }

    /// Connect and register without exposing a usable server.
    ///
    /// The returned pending value performs no metadata fetch. The caller must
    /// durably commit the complete per-authority trust record before activation.
    pub(crate) async fn begin_enrollment(
        url: CanonicalServerUrl,
        client_name: &str,
        client_uuid: Option<Uuid>,
        expected_certificate: Option<[u8; 32]>,
        enrollment_token: Option<&str>,
        publisher_policy: PublisherPolicy,
    ) -> Result<PendingServer, ServerRequest> {
        let mut transport = Self::make_transport(&url, client_uuid, expected_certificate).await?;
        tracing::debug!("Connected to server");
        let certificate_fingerprint = transport.certificate_fingerprint();
        let assigned_uuid =
            register_transport(&mut transport, client_name, client_uuid, enrollment_token).await?;

        Ok(PendingServer {
            inner: Some(PendingServerInner {
                url,
                certificate_fingerprint,
                publisher_policy,
                client_uuid: assigned_uuid,
                client_name: client_name.to_owned(),
                enrollment_token: enrollment_token.map(str::to_owned),
                transport,
                timeout: Duration::new(5, 0),
            }),
        })
    }

    /// Get the client uuid
    #[must_use]
    pub fn client_uuid(&self) -> Uuid {
        self.client_uuid.unwrap_or(Uuid::nil())
    }

    async fn make_transport(
        url: &CanonicalServerUrl,
        uuid: Option<Uuid>,
        expected_certificate: Option<[u8; 32]>,
    ) -> Result<TransportClient, ServerRequest> {
        QuicTransportClient::connect(
            url,
            uuid.filter(|uuid| !uuid.is_nil()),
            expected_certificate,
        )
        .await
        .map(TransportClient::Quic)
    }

    /// Register a new client
    pub async fn register(&mut self) -> Result<Uuid, ServerRequest> {
        let transport = self.shared.read().await.transport.clone();
        let uuid = register_transport(
            &transport,
            &self.client_name,
            self.client_uuid,
            self.enrollment_token.as_deref(),
        )
        .await?;
        self.client_uuid = Some(uuid);
        Ok(uuid)
    }

    /// Get the server metadata
    pub async fn metadata(&self) -> ServerMetadata {
        let snapshot = Arc::clone(&self.shared.read().await.metadata);
        tokio::task::spawn_blocking(move || (*snapshot).clone())
            .await
            .expect("immutable metadata snapshot cloning cannot panic")
    }

    pub(crate) async fn metadata_item(&self, artifact_id: &str) -> Option<ItemMetadata> {
        self.shared
            .read()
            .await
            .metadata
            .items
            .get(artifact_id)
            .cloned()
    }

    /// Get the last time metadata was fetched from the server
    pub async fn last_update(&self) -> Instant {
        self.shared.read().await.last_update
    }

    /// Fetch metadata from server
    async fn fetch(&self) -> Result<(), ServerRequest> {
        tracing::trace!("Starting `Fetch` request");
        assert!(self.client_uuid.is_some());
        let transport = self.shared.read().await.transport.clone();
        let res = transport
            .fetch(distd_core::proto::ClientKeepAlive {})
            .await?;
        tracing::trace!("Parsed `Fetch` response");

        let new_metadata = tokio::task::spawn_blocking(move || {
            bitcode::deserialize::<ServerMetadata>(&res.serialized)
        })
        .await
        .map_err(|_| ServerRequest::MetadataWorker)??;
        let candidate = Arc::new(new_metadata);
        let (retired, installed_item_count) = {
            let mut shared = self.shared.write().await;
            shared.last_update = Instant::now();
            if shared.metadata.as_ref() != candidate.as_ref() {
                let item_count = candidate.items.len();
                (
                    std::mem::replace(&mut shared.metadata, candidate),
                    Some(item_count),
                )
            } else {
                (candidate, None)
            }
        };
        tokio::task::spawn_blocking(move || drop(retired))
            .await
            .map_err(|_| ServerRequest::MetadataWorker)?;
        if let Some(item_count) = installed_item_count {
            tracing::trace!(item_count, "installed new metadata snapshot");
        }

        Ok(())
    }

    /// PPSPP-style sync: manifest handshake → bitfield → chunk transfer.
    ///
    /// Returns `(manifest, chunk_hashes, received_chunks)` where received_chunks
    /// are ordered exactly like the missing leaf chunks implied by `chunk_hashes`
    /// and the provided `local_hashes`.
    pub async fn sync_artifact(
        &self,
        artifact_id: &str,
        local_hashes: &[Hash],
    ) -> Result<(Manifest, Vec<ChunkInfo>, Vec<Vec<u8>>, u64), ServerRequest> {
        let expected = self
            .metadata_item(artifact_id)
            .await
            .ok_or(ServerRequest::UnexpectedMessage)?;
        let (manifest, chunk_infos, received, wire_bytes, _) = self
            .sync_artifact_expected_shared(artifact_id.to_owned(), expected, local_hashes.to_vec())
            .await?;
        Ok((
            manifest,
            chunk_infos,
            received.into_iter().map(ReceivedChunk::into_vec).collect(),
            wire_bytes,
        ))
    }

    /// Shared-backing sync path used by the built-in filesystem client.
    pub async fn sync_artifact_shared(
        &self,
        artifact_id: &str,
        local_hashes: &[Hash],
    ) -> Result<(Manifest, Vec<ChunkInfo>, Vec<ReceivedChunk>, u64), ServerRequest> {
        let expected = self
            .metadata_item(artifact_id)
            .await
            .ok_or(ServerRequest::UnexpectedMessage)?;
        let (manifest, chunks, received, wire_bytes, _) = self
            .sync_artifact_expected_shared(artifact_id.to_owned(), expected, local_hashes.to_vec())
            .await?;
        Ok((manifest, chunks, received, wire_bytes))
    }

    pub(crate) async fn sync_artifact_expected_shared(
        &self,
        requested_artifact_id: String,
        expected: ItemMetadata,
        local_hashes: Vec<Hash>,
    ) -> Result<(Manifest, Vec<ChunkInfo>, Vec<ReceivedChunk>, u64, Vec<Hash>), ServerRequest> {
        require_requested_catalog_identity(&requested_artifact_id, &expected)?;
        let transport = self.shared.read().await.transport.clone();
        let manifest_request = proto::ManifestRequest {
            artifact_id: requested_artifact_id,
            version: Some(u64::from(expected.revision)),
        };
        let pending = transport.open_sync(manifest_request).await?;
        verify_and_transfer_pending(
            pending,
            expected,
            local_hashes,
            self.publisher_policy.clone(),
        )
        .await
    }

    /// Fetch metadata from server in a loop
    pub async fn fetch_loop(mut self) {
        loop {
            tokio::time::sleep(self.timeout).await;
            if self.fetch().await.is_err() {
                // try to re-establish connection to server
                if let Ok(client) = Self::make_transport(
                    &self.url,
                    Some(self.client_uuid()),
                    Some(self.certificate_pin),
                )
                .await
                {
                    tracing::info!("Connected to server");
                    self.shared.write().await.transport = client;
                    if let Err(error) = self.register().await {
                        tracing::warn!("Cannot re-enroll with server: {error}");
                    }
                } else {
                    tracing::warn!(
                        "Cannot connect to server, retrying in {} seconds",
                        self.timeout.as_secs()
                    );
                }
            }
        }
    }
}

async fn verify_and_transfer_pending(
    pending: PendingSync,
    expected: ItemMetadata,
    local_hashes: Vec<Hash>,
    policy: PublisherPolicy,
) -> Result<(Manifest, Vec<ChunkInfo>, Vec<ReceivedChunk>, u64, Vec<Hash>), ServerRequest> {
    verify_and_transfer_pending_with(pending, move |response| {
        verify_manifest_response(response, &expected, local_hashes, &policy)
    })
    .await
}

async fn verify_and_transfer_pending_with<Verify>(
    pending: PendingSync,
    verify: Verify,
) -> Result<(Manifest, Vec<ChunkInfo>, Vec<ReceivedChunk>, u64, Vec<Hash>), ServerRequest>
where
    Verify: FnOnce(proto::ManifestResponse) -> Result<VerifiedManifestParts, PublisherAuthenticityError>
        + Send
        + 'static,
{
    let PendingSync {
        manifest,
        continuation,
    } = pending;
    let verified = tokio::task::spawn_blocking(move || manifest.decode().and_then(verify)).await;
    let (manifest, chunks, chunk_sizes, possession, publisher_fingerprint, local_hashes) =
        match verified {
            Ok(Ok(verified)) => verified,
            Ok(Err(error)) => {
                continuation.abort();
                return Err(error.into());
            }
            Err(_) => {
                continuation.abort();
                return Err(ServerRequest::PublisherVerificationWorker);
            }
        };
    VerifiedSync {
        manifest,
        chunks,
        chunk_sizes,
        possession,
        publisher_fingerprint,
        local_hashes,
        continuation,
    }
    .transfer()
    .await
}

fn require_requested_catalog_identity(
    requested_artifact_id: &str,
    expected: &ItemMetadata,
) -> Result<(), PublisherAuthenticityError> {
    if expected.artifact_id == requested_artifact_id {
        Ok(())
    } else {
        Err(PublisherAuthenticityError::IdentityMismatch)
    }
}

async fn register_transport(
    transport: &TransportClient,
    client_name: &str,
    prior_uuid: Option<Uuid>,
    enrollment_token: Option<&str>,
) -> Result<Uuid, ServerRequest> {
    tracing::trace!("Starting `Register` request");
    let response = transport
        .register(distd_core::proto::ClientRegister {
            name: client_name.to_owned(),
            version: VERSION.to_string(),
            uuid: prior_uuid
                .filter(|uuid| !uuid.is_nil())
                .map(|uuid| uuid.as_bytes().to_vec()),
            enrollment_token: enrollment_token.map(str::to_owned),
        })
        .await?;
    tracing::trace!("Parsed `Register` response");

    let uuid = assigned_uuid(response.uuid)?;
    require_same_prior_identity(prior_uuid, uuid)?;
    transport.set_client_uuid(uuid).await;
    tracing::info!("Server assigned client identity");
    Ok(uuid)
}

fn assigned_uuid(bytes: Option<Vec<u8>>) -> Result<Uuid, ServerRequest> {
    let bytes = bytes.ok_or(ServerRequest::MissingUuid)?;
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| ServerRequest::BadUuid)?;
    let uuid = Uuid::from_bytes(bytes);
    if uuid.is_nil() {
        return Err(ServerRequest::BadUuid);
    }
    Ok(uuid)
}

fn require_same_prior_identity(
    prior_uuid: Option<Uuid>,
    assigned_uuid: Uuid,
) -> Result<(), ServerRequest> {
    if prior_uuid.is_some_and(|expected| expected != assigned_uuid) {
        return Err(ServerRequest::ClientIdentityChanged);
    }
    Ok(())
}

fn receipt_matches_pending(
    receipt: &DurableTrustReceipt,
    url: &CanonicalServerUrl,
    certificate_pin: [u8; 32],
    client_uuid: Uuid,
) -> bool {
    receipt.matches(
        url,
        &ServerTrustRecord {
            certificate_pin,
            client_uuid,
        },
    )
}

fn redacted_token(token: Option<&str>) -> Option<&'static str> {
    token.map(|_| "[REDACTED]")
}

fn publisher_envelope_from_transport(
    public_key: &[u8],
    signature: &[u8],
) -> Result<Option<PublisherSignatureEnvelope>, PublisherAuthenticityError> {
    match (public_key, signature) {
        ([], []) => Ok(None),
        (public_key, signature) if !public_key.is_empty() && !signature.is_empty() => {
            Ok(Some(PublisherSignatureEnvelope::from_parts(
                PublisherPublicKey::try_from(public_key)
                    .map_err(|_| PublisherAuthenticityError::MalformedAttestation)?,
                PublisherSignature::try_from(signature)
                    .map_err(|_| PublisherAuthenticityError::MalformedAttestation)?,
            )))
        }
        _ => Err(PublisherAuthenticityError::MalformedAttestation),
    }
}

fn verify_manifest_response(
    response: proto::ManifestResponse,
    expected: &ItemMetadata,
    local_hashes: Vec<Hash>,
    policy: &PublisherPolicy,
) -> Result<VerifiedManifestParts, PublisherAuthenticityError> {
    let envelope = publisher_envelope_from_transport(
        &response.publisher_public_key,
        &response.publisher_signature,
    )?;
    match envelope.as_ref() {
        None if !policy.permits_unsigned() => {
            return Err(PublisherAuthenticityError::MissingAttestation)
        }
        Some(envelope)
            if !policy.permits_unsigned() && !policy.contains_exact_key(envelope.public_key()) =>
        {
            return Err(PublisherAuthenticityError::UntrustedPublisher)
        }
        _ => {}
    }

    let root_hash = Hash::from_bytes(
        response
            .root_hash
            .try_into()
            .map_err(|_| PublisherAuthenticityError::MalformedAttestation)?,
    );
    if response.artifact_id != expected.artifact_id
        || response.version != u64::from(expected.revision)
        || root_hash != expected.root.hash
        || response.total_size != expected.root.size
    {
        return Err(PublisherAuthenticityError::IdentityMismatch);
    }
    let chunk_algorithm = ChunkAlgorithm::try_from_proto_exact(response.chunk_algorithm.as_ref())
        .map_err(|_| PublisherAuthenticityError::MalformedAttestation)?;
    if response.chunk_sizes.len() != response.chunk_hashes.len() {
        return Err(PublisherAuthenticityError::MalformedAttestation);
    }
    let chunks = response
        .chunk_hashes
        .into_iter()
        .zip(response.chunk_sizes)
        .map(|(bytes, size)| {
            let hash = Hash::from_bytes(
                bytes
                    .try_into()
                    .map_err(|_| PublisherAuthenticityError::MalformedAttestation)?,
            );
            Ok(ChunkInfo {
                hash,
                size: u64::from(size),
            })
        })
        .collect::<Result<Vec<_>, PublisherAuthenticityError>>()?;
    let manifest = Manifest {
        artifact_id: response.artifact_id,
        version: response.version,
        root_hash,
        total_size: response.total_size,
        chunk_count: response.chunk_count,
        chunk_size: response.chunk_size,
        chunk_algorithm,
        entries: response
            .entries
            .into_iter()
            .map(|entry| FileEntry {
                relative_path: entry.relative_path,
                size: entry.size,
                chunk_range: (entry.chunk_start, entry.chunk_end),
            })
            .collect(),
    };
    let publisher_fingerprint = policy.authorize_manifest(envelope.as_ref(), &manifest, &chunks)?;

    let chunk_sizes = chunks
        .iter()
        .map(|chunk| {
            usize::try_from(chunk.size)
                .map_err(|_| PublisherAuthenticityError::MalformedAttestation)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let available_hashes = local_hashes.iter().copied().collect::<HashSet<_>>();
    let mut bitfield = distd_core::possession::Bitfield::empty(manifest.chunk_count);
    for (index, chunk) in chunks.iter().enumerate() {
        if available_hashes.contains(&chunk.hash) {
            let index = u32::try_from(index)
                .map_err(|_| PublisherAuthenticityError::MalformedAttestation)?;
            bitfield.set(index);
        }
    }
    Ok((
        manifest,
        chunks,
        chunk_sizes,
        proto::PossessionBitfield {
            bitfield: bitfield.to_bytes(),
        },
        publisher_fingerprint,
        local_hashes,
    ))
}

async fn fetch_metadata(transport: &TransportClient) -> Result<ServerMetadata, ServerRequest> {
    tracing::trace!("Starting `Fetch` request");
    let response = transport
        .fetch(distd_core::proto::ClientKeepAlive {})
        .await?;
    tracing::trace!("Parsed `Fetch` response");
    tokio::task::spawn_blocking(move || bitcode::deserialize(&response.serialized))
        .await
        .map_err(|_| ServerRequest::MetadataWorker)?
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::{
        assigned_uuid, publisher_envelope_from_transport, receipt_matches_pending, redacted_token,
        require_requested_catalog_identity, require_same_prior_identity,
        verify_and_transfer_pending, verify_and_transfer_pending_with, verify_manifest_response,
        ObservedSyncContinuation, ObservedSyncSession, PendingManifest, PendingServer, PendingSync,
        SyncContinuation,
    };
    use crate::error::ServerRequest;
    use crate::{
        persistence::{ClientPersistentState, ServerTrustRecord},
        publisher::{PublisherAuthenticityError, PublisherPolicy},
        server_url::CanonicalServerUrl,
    };
    use distd_core::{
        chunk_storage::{hashmap_storage::HashMapStorage, ChunkStorage},
        chunks::ChunkAlgorithm,
        hash::Hash,
        proto,
        signature::{
            PublisherPublicKey, PublisherSignature, PublisherSignatureEnvelope, PublisherStatement,
        },
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};

    fn signed_manifest_fixture() -> (
        proto::ManifestResponse,
        distd_core::metadata::Item,
        PublisherPolicy,
    ) {
        let storage = HashMapStorage::default();
        let item = storage
            .create_item(
                "signed-client-artifact".to_string(),
                std::path::PathBuf::from("signed-client-artifact.bin"),
                0,
                None,
                bytes::Bytes::from_static(b"publisher verified client payload"),
                ChunkAlgorithm::Fixed256K,
            )
            .unwrap();
        signed_response_for_item(item, [17; 32])
    }

    fn signed_directory_fixture() -> (
        proto::ManifestResponse,
        distd_core::metadata::Item,
        PublisherPolicy,
    ) {
        let storage = HashMapStorage::default();
        let item = storage
            .prepare_revision_from_files(
                "signed-client-directory".to_string(),
                "signed-client-directory".to_string(),
                std::path::PathBuf::from("signed-client-directory"),
                0,
                None,
                vec![
                    (
                        std::path::PathBuf::from("a.bin"),
                        bytes::Bytes::from_static(b"first signed directory entry"),
                    ),
                    (
                        std::path::PathBuf::from("nested/b.bin"),
                        bytes::Bytes::from_static(b"second signed directory entry"),
                    ),
                ],
                ChunkAlgorithm::Fixed256K,
            )
            .unwrap();
        signed_response_for_item(item, [23; 32])
    }

    fn signed_response_for_item(
        item: distd_core::item::Item,
        seed: [u8; 32],
    ) -> (
        proto::ManifestResponse,
        distd_core::metadata::Item,
        PublisherPolicy,
    ) {
        let pair = Ed25519KeyPair::from_seed_unchecked(&seed).unwrap();
        let statement = PublisherStatement::from_manifest(&item.manifest, &item.chunks).unwrap();
        let envelope = PublisherSignatureEnvelope::from_parts(
            PublisherPublicKey::try_from(pair.public_key().as_ref()).unwrap(),
            PublisherSignature::try_from(pair.sign(statement.as_bytes()).as_ref()).unwrap(),
        );
        let policy = PublisherPolicy::from_config(
            &[envelope
                .public_key()
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect()],
            false,
        )
        .unwrap();
        let response = proto::ManifestResponse {
            artifact_id: item.manifest.artifact_id.clone(),
            version: item.manifest.version,
            root_hash: item.manifest.root_hash.as_bytes().to_vec(),
            total_size: item.manifest.total_size,
            chunk_count: item.manifest.chunk_count,
            chunk_size: item.manifest.chunk_size,
            chunk_hashes: item
                .chunks
                .iter()
                .map(|chunk| chunk.hash.as_bytes().to_vec())
                .collect(),
            entries: item
                .manifest
                .entries
                .iter()
                .map(|entry| proto::FileEntry {
                    relative_path: entry.relative_path.clone(),
                    size: entry.size,
                    chunk_start: entry.chunk_range.0,
                    chunk_end: entry.chunk_range.1,
                })
                .collect(),
            chunk_algorithm: Some(item.manifest.chunk_algorithm.to_proto()),
            chunk_sizes: item
                .chunks
                .iter()
                .map(|chunk| u32::try_from(chunk.size).unwrap())
                .collect(),
            publisher_public_key: envelope.public_key().as_bytes().to_vec(),
            publisher_signature: envelope.signature().as_bytes().to_vec(),
        };
        (response, item.metadata, policy)
    }

    fn reference_item(chunk_count: usize, directory: bool) -> distd_core::item::Item {
        let chunks = (0..chunk_count)
            .map(|index| distd_core::chunks::ChunkInfo {
                hash: distd_core::hash::hash(&index.to_le_bytes()),
                size: 1,
            })
            .collect::<Vec<_>>();
        let mut partials = chunks.clone();
        while partials.len() > 1 {
            let count = partials.len();
            for (target, source) in (0..count - 1).step_by(2).enumerate() {
                partials[target] = distd_core::chunks::ChunkInfo {
                    hash: distd_core::hash::merge_hashes(
                        &partials[source].hash,
                        &partials[source + 1].hash,
                    ),
                    size: partials[source].size + partials[source + 1].size,
                };
            }
            let half = count / 2;
            if count % 2 == 1 {
                partials.swap(half, count - 1);
                partials.truncate(half + 1);
            } else {
                partials.truncate(half);
            }
        }
        let root = partials.pop().unwrap();
        let entries = if directory {
            (0..chunk_count)
                .map(|index| distd_core::item::FileEntry {
                    relative_path: format!("entry-{index:05}.bin"),
                    size: 1,
                    chunk_range: (
                        u32::try_from(index).unwrap(),
                        u32::try_from(index + 1).unwrap(),
                    ),
                })
                .collect()
        } else {
            Vec::new()
        };
        let artifact_id = format!(
            "client-reference-{}-{chunk_count}",
            if directory { "directory" } else { "file" }
        );
        distd_core::item::Item::make_with_entries(
            artifact_id.clone(),
            std::path::PathBuf::from(artifact_id),
            0,
            None,
            root,
            chunks,
            entries,
            ChunkAlgorithm::Fixed256K,
        )
        .unwrap()
    }

    fn percentile_95(samples: &[std::time::Duration]) -> std::time::Duration {
        percentile(samples, 95)
    }

    fn percentile(samples: &[std::time::Duration], percentile: usize) -> std::time::Duration {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        sorted[(sorted.len() * percentile).div_ceil(100) - 1]
    }

    #[test]
    fn assigned_enrollment_identity_must_be_present_exact_and_non_nil() {
        assert!(matches!(
            assigned_uuid(None),
            Err(ServerRequest::MissingUuid)
        ));
        assert!(matches!(
            assigned_uuid(Some(vec![1; 15])),
            Err(ServerRequest::BadUuid)
        ));
        assert!(matches!(
            assigned_uuid(Some(vec![0; 16])),
            Err(ServerRequest::BadUuid)
        ));

        let expected = uuid::Uuid::from_bytes([7; 16]);
        assert_eq!(assigned_uuid(Some(vec![7; 16])).unwrap(), expected);
        assert!(require_same_prior_identity(None, expected).is_ok());
        assert!(require_same_prior_identity(Some(expected), expected).is_ok());
        assert!(matches!(
            require_same_prior_identity(Some(uuid::Uuid::from_bytes([8; 16])), expected),
            Err(ServerRequest::ClientIdentityChanged)
        ));
    }

    #[test]
    fn enrollment_token_debug_value_is_always_redacted() {
        let secret = "enrollment-secret-that-must-never-be-formatted";
        let formatted = format!("{:?}", redacted_token(Some(secret)));
        assert!(formatted.contains("[REDACTED]"));
        assert!(!formatted.contains(secret));
        assert_eq!(redacted_token(None), None);
    }

    #[test]
    fn publisher_transport_tuple_is_exactly_all_or_none() {
        assert!(matches!(
            publisher_envelope_from_transport(&[], &[]),
            Ok(None)
        ));
        let envelope = publisher_envelope_from_transport(&[3; 32], &[5; 64])
            .unwrap()
            .unwrap();
        assert_eq!(envelope.public_key().as_bytes(), &[3; 32]);
        assert_eq!(envelope.signature().as_bytes(), &[5; 64]);

        for (key, signature) in [
            (vec![], vec![5; 64]),
            (vec![3; 32], vec![]),
            (vec![3; 31], vec![5; 64]),
            (vec![3; 33], vec![5; 64]),
            (vec![3; 32], vec![5; 63]),
            (vec![3; 32], vec![5; 65]),
        ] {
            assert!(matches!(
                publisher_envelope_from_transport(&key, &signature),
                Err(PublisherAuthenticityError::MalformedAttestation)
            ));
        }
    }

    #[test]
    fn signed_manifest_is_verified_before_possession_is_constructed() {
        let (response, expected, policy) = signed_manifest_fixture();
        let available = Hash::from_bytes(response.chunk_hashes[0].clone().try_into().unwrap());
        let (manifest, chunks, chunk_sizes, possession, fingerprint, returned_hashes) =
            verify_manifest_response(response, &expected, vec![available], &policy).unwrap();

        assert_eq!(manifest.artifact_id, expected.artifact_id);
        assert_eq!(manifest.version, u64::from(expected.revision));
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunk_sizes, vec![usize::try_from(chunks[0].size).unwrap()]);
        assert_eq!(returned_hashes, vec![available]);
        assert!(fingerprint.is_some());
        let bitfield = distd_core::possession::Bitfield::from_bytes(&possession.bitfield).unwrap();
        assert_eq!(bitfield.chunk_count(), 1);
        assert!(bitfield.has(0));
    }

    #[test]
    fn signed_directory_entries_and_detected_bzip2_profile_use_the_same_boundary() {
        let (response, expected, policy) = signed_directory_fixture();
        let (manifest, chunks, _, possession, fingerprint, _) =
            verify_manifest_response(response.clone(), &expected, Vec::new(), &policy).unwrap();
        assert_eq!(manifest.entries.len(), 2);
        assert_eq!(chunks.len(), 2);
        assert!(fingerprint.is_some());
        assert!(
            distd_core::possession::Bitfield::from_bytes(&possession.bitfield)
                .unwrap()
                .is_empty()
        );

        let entry_mutations = [
            {
                let mut response = response.clone();
                response.entries.reverse();
                response
            },
            {
                let mut response = response.clone();
                response.entries[0].relative_path = ".distd-activation-v1/forged".to_string();
                response
            },
            {
                let mut response = response.clone();
                response.entries[0].size += 1;
                response
            },
            {
                let mut response = response.clone();
                response.entries[1].chunk_start = 0;
                response
            },
        ];
        for mutated in entry_mutations {
            assert_eq!(
                verify_manifest_response(mutated, &expected, Vec::new(), &policy),
                Err(PublisherAuthenticityError::InvalidSignature)
            );
        }

        let data = bytes::Bytes::from_static(b"BZh91AY&SYsigned-bzip2-profile");
        let algorithm = ChunkAlgorithm::from_policy(
            "auto".parse().unwrap(),
            std::path::Path::new("archive.tar.bz2"),
            data.as_ref(),
            false,
        );
        assert!(matches!(algorithm, ChunkAlgorithm::FixedAligned { .. }));
        let item = HashMapStorage::default()
            .prepare_revision(
                "signed-bzip2".to_string(),
                "signed-bzip2".to_string(),
                std::path::PathBuf::from("archive.tar.bz2"),
                0,
                None,
                data,
                algorithm,
            )
            .unwrap();
        let (response, expected, policy) = signed_response_for_item(item, [31; 32]);
        let (manifest, _, _, _, _, _) =
            verify_manifest_response(response, &expected, Vec::new(), &policy).unwrap();
        assert_eq!(manifest.chunk_algorithm, algorithm);
    }

    #[test]
    fn every_canonical_chunk_algorithm_crosses_the_client_authenticity_boundary() {
        let algorithms = [
            ChunkAlgorithm::Fixed256K,
            ChunkAlgorithm::fixed_1m(),
            ChunkAlgorithm::fastcdc_default(),
            ChunkAlgorithm::image_default(),
            ChunkAlgorithm::archive_default(),
        ];
        for (index, algorithm) in algorithms.into_iter().enumerate() {
            let artifact_id = format!("signed-algorithm-{index}");
            let item = HashMapStorage::default()
                .prepare_revision(
                    artifact_id.clone(),
                    artifact_id.clone(),
                    std::path::PathBuf::from(&artifact_id),
                    0,
                    None,
                    bytes::Bytes::from(vec![u8::try_from(index).unwrap(); 4_097]),
                    algorithm,
                )
                .unwrap();
            let (response, expected, policy) =
                signed_response_for_item(item, [u8::try_from(index + 71).unwrap(); 32]);
            let (manifest, _, _, _, fingerprint, _) =
                verify_manifest_response(response, &expected, Vec::new(), &policy).unwrap();
            assert_eq!(manifest.chunk_algorithm, algorithm);
            assert!(fingerprint.is_some());
        }
    }

    #[test]
    fn requested_catalog_key_must_equal_the_catalog_value_identity() {
        let (_, expected, _) = signed_manifest_fixture();
        assert!(require_requested_catalog_identity(&expected.artifact_id, &expected).is_ok());
        assert_eq!(
            require_requested_catalog_identity("catalog-alias", &expected),
            Err(PublisherAuthenticityError::IdentityMismatch)
        );
    }

    #[test]
    fn publisher_trust_and_crypto_failures_are_distinct_and_fail_closed() {
        let (response, expected, policy) = signed_manifest_fixture();

        let mut missing = response.clone();
        missing.publisher_public_key.clear();
        missing.publisher_signature.clear();
        assert_eq!(
            verify_manifest_response(missing, &expected, Vec::new(), &policy),
            Err(PublisherAuthenticityError::MissingAttestation)
        );

        let mut missing_key = response.clone();
        missing_key.publisher_public_key.clear();
        let mut missing_signature = response.clone();
        missing_signature.publisher_signature.clear();
        for partial in [missing_key, missing_signature] {
            assert_eq!(
                verify_manifest_response(partial, &expected, Vec::new(), &policy),
                Err(PublisherAuthenticityError::MalformedAttestation)
            );
        }

        let mut unknown = response.clone();
        unknown.publisher_public_key = vec![99; 32];
        unknown.publisher_signature.fill(0);
        assert_eq!(
            verify_manifest_response(unknown, &expected, Vec::new(), &policy),
            Err(PublisherAuthenticityError::UntrustedPublisher)
        );

        let mut invalid = response;
        invalid.publisher_signature[0] ^= 1;
        assert_eq!(
            verify_manifest_response(invalid, &expected, Vec::new(), &policy),
            Err(PublisherAuthenticityError::InvalidSignature)
        );
    }

    #[test]
    fn response_identity_and_typed_manifest_are_bound_before_possession() {
        let (response, expected, policy) = signed_manifest_fixture();

        let identity_mutations = [
            {
                let mut response = response.clone();
                response.artifact_id.push_str("-substitution");
                response
            },
            {
                let mut response = response.clone();
                response.version += 1;
                response
            },
            {
                let mut response = response.clone();
                response.root_hash[0] ^= 1;
                response
            },
            {
                let mut response = response.clone();
                response.total_size += 1;
                response
            },
        ];
        for mutated in identity_mutations {
            assert_eq!(
                verify_manifest_response(mutated, &expected, Vec::new(), &policy),
                Err(PublisherAuthenticityError::IdentityMismatch)
            );
        }

        let malformed_mutations = [
            {
                let mut response = response.clone();
                response.chunk_hashes[0].pop();
                response
            },
            {
                let mut response = response.clone();
                response.chunk_sizes.clear();
                response
            },
            {
                let mut response = response.clone();
                response.chunk_algorithm = None;
                response
            },
        ];
        for mutated in malformed_mutations {
            assert_eq!(
                verify_manifest_response(mutated, &expected, Vec::new(), &policy),
                Err(PublisherAuthenticityError::MalformedAttestation)
            );
        }

        let mut signed_field_mutation = response;
        signed_field_mutation.chunk_sizes[0] += 1;
        assert_eq!(
            verify_manifest_response(signed_field_mutation, &expected, Vec::new(), &policy,),
            Err(PublisherAuthenticityError::InvalidSignature)
        );
    }

    #[test]
    fn explicit_unsigned_mode_still_requires_a_canonical_manifest() {
        let (mut response, expected, _) = signed_manifest_fixture();
        response.publisher_public_key.clear();
        response.publisher_signature.clear();
        let policy = PublisherPolicy::from_config(&[], true).unwrap();
        assert!(
            verify_manifest_response(response.clone(), &expected, Vec::new(), &policy,).is_ok()
        );

        response.chunk_count += 1;
        assert_eq!(
            verify_manifest_response(response, &expected, Vec::new(), &policy),
            Err(PublisherAuthenticityError::InvalidSignature)
        );
    }

    #[tokio::test]
    async fn possession_capability_is_never_invoked_before_authenticity_succeeds() {
        let (response, expected, policy) = signed_manifest_fixture();
        let local_hash = Hash::from_bytes(response.chunk_hashes[0].clone().try_into().unwrap());

        let rejected_observer = std::sync::Arc::new(ObservedSyncContinuation::default());
        let mut rejected_response = response.clone();
        rejected_response.publisher_signature[0] ^= 1;
        let rejected = verify_and_transfer_pending(
            PendingSync {
                manifest: PendingManifest::Decoded(rejected_response),
                continuation: SyncContinuation::Observed(ObservedSyncSession::new(
                    rejected_observer.clone(),
                )),
            },
            expected.clone(),
            vec![local_hash],
            policy.clone(),
        )
        .await;
        assert!(matches!(
            rejected,
            Err(ServerRequest::PublisherAuthenticity(
                PublisherAuthenticityError::InvalidSignature
            ))
        ));
        assert_eq!(
            rejected_observer
                .possession_sends
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            rejected_observer
                .aborts
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        let accepted_observer = std::sync::Arc::new(ObservedSyncContinuation::default());
        let accepted = verify_and_transfer_pending(
            PendingSync {
                manifest: PendingManifest::Decoded(response),
                continuation: SyncContinuation::Observed(ObservedSyncSession::new(
                    accepted_observer.clone(),
                )),
            },
            expected,
            vec![local_hash],
            policy,
        )
        .await
        .unwrap();
        assert!(
            accepted.2.is_empty(),
            "complete possession receives no chunks"
        );
        assert_eq!(
            accepted_observer
                .possession_sends
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            accepted_observer
                .aborts
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_during_blocking_verification_aborts_without_possession() {
        let (response, expected, policy) = signed_manifest_fixture();
        let local_hash = Hash::from_bytes(response.chunk_hashes[0].clone().try_into().unwrap());
        let observer = std::sync::Arc::new(ObservedSyncContinuation::default());
        let release =
            std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let release_worker = release.clone();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);

        let task = tokio::spawn(verify_and_transfer_pending_with(
            PendingSync {
                manifest: PendingManifest::Decoded(response),
                continuation: SyncContinuation::Observed(ObservedSyncSession::new(
                    observer.clone(),
                )),
            },
            move |response| {
                started_tx.send(()).unwrap();
                let (lock, condition) = &*release_worker;
                let released = lock.lock().unwrap();
                let _released = condition
                    .wait_while(released, |released| !*released)
                    .unwrap();
                verify_manifest_response(response, &expected, vec![local_hash], &policy)
            },
        ));
        started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        {
            let (lock, condition) = &*release;
            *lock.lock().unwrap() = true;
            condition.notify_all();
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert_eq!(
            observer
                .possession_sends
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(observer.aborts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn signed_syncs_overlap_without_sharing_request_or_catalog_authority() {
        fn hashes(response: &proto::ManifestResponse) -> Vec<Hash> {
            response
                .chunk_hashes
                .iter()
                .map(|hash| Hash::from_bytes(hash.clone().try_into().unwrap()))
                .collect()
        }

        let (file_response, mut catalog_file, file_policy) = signed_manifest_fixture();
        let (directory_response, catalog_directory, directory_policy) = signed_directory_fixture();
        let file_observer = std::sync::Arc::new(ObservedSyncContinuation::default());
        let directory_observer = std::sync::Arc::new(ObservedSyncContinuation::default());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let inflight_file = catalog_file.clone();
        let file_hashes = hashes(&file_response);
        let directory_hashes = hashes(&directory_response);

        let file_task = tokio::spawn(verify_and_transfer_pending_with(
            PendingSync {
                manifest: PendingManifest::Decoded(file_response),
                continuation: SyncContinuation::Observed(ObservedSyncSession::new(
                    file_observer.clone(),
                )),
            },
            {
                let barrier = barrier.clone();
                move |response| {
                    barrier.wait();
                    verify_manifest_response(response, &inflight_file, file_hashes, &file_policy)
                }
            },
        ));
        let directory_task = tokio::spawn(verify_and_transfer_pending_with(
            PendingSync {
                manifest: PendingManifest::Decoded(directory_response),
                continuation: SyncContinuation::Observed(ObservedSyncSession::new(
                    directory_observer.clone(),
                )),
            },
            {
                let barrier = barrier.clone();
                move |response| {
                    barrier.wait();
                    verify_manifest_response(
                        response,
                        &catalog_directory,
                        directory_hashes,
                        &directory_policy,
                    )
                }
            },
        ));

        // A refreshed advisory catalog cannot mutate the ItemMetadata snapshot
        // already owned by the exact-version request worker.
        catalog_file.artifact_id = "refreshed-catalog-value".to_string();
        let (file, directory) = tokio::join!(file_task, directory_task);
        file.unwrap().unwrap();
        directory.unwrap().unwrap();
        assert_eq!(
            file_observer
                .possession_sends
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            directory_observer
                .possession_sends
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );

        let (valid_response, expected, policy) = signed_manifest_fixture();
        let mut invalid_response = valid_response.clone();
        invalid_response.publisher_signature[0] ^= 1;
        let valid_observer = std::sync::Arc::new(ObservedSyncContinuation::default());
        let invalid_observer = std::sync::Arc::new(ObservedSyncContinuation::default());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let valid_hashes = hashes(&valid_response);

        let valid = verify_and_transfer_pending_with(
            PendingSync {
                manifest: PendingManifest::Decoded(valid_response),
                continuation: SyncContinuation::Observed(ObservedSyncSession::new(
                    valid_observer.clone(),
                )),
            },
            {
                let barrier = barrier.clone();
                let expected = expected.clone();
                let policy = policy.clone();
                move |response| {
                    barrier.wait();
                    verify_manifest_response(response, &expected, valid_hashes, &policy)
                }
            },
        );
        let invalid = verify_and_transfer_pending_with(
            PendingSync {
                manifest: PendingManifest::Decoded(invalid_response),
                continuation: SyncContinuation::Observed(ObservedSyncSession::new(
                    invalid_observer.clone(),
                )),
            },
            move |response| {
                barrier.wait();
                verify_manifest_response(response, &expected, Vec::new(), &policy)
            },
        );
        let (valid, invalid) = tokio::join!(valid, invalid);
        valid.unwrap();
        assert!(matches!(
            invalid,
            Err(ServerRequest::PublisherAuthenticity(
                PublisherAuthenticityError::InvalidSignature
            ))
        ));
        assert_eq!(
            valid_observer
                .possession_sends
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(
            invalid_observer
                .possession_sends
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert_eq!(
            invalid_observer
                .aborts
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "release-only local publisher-verification reference"]
    async fn publisher_client_verification_reference_performance() {
        assert!(
            !cfg!(debug_assertions),
            "run this ignored reference with --release"
        );

        async fn campaign(
            chunk_count: usize,
            directory: bool,
            observe_heartbeat: bool,
        ) -> (
            Vec<std::time::Duration>,
            Vec<std::time::Duration>,
            Vec<std::time::Duration>,
        ) {
            let item = reference_item(chunk_count, directory);
            let (response, expected, policy) = signed_response_for_item(item, [47; 32]);
            let local_hashes = response
                .chunk_hashes
                .iter()
                .step_by(2)
                .map(|hash| Hash::from_bytes(hash.clone().try_into().unwrap()))
                .collect::<Vec<_>>();
            let frame = distd_core::utils::frame::encode_length_delimited(&proto::SyncMessage {
                msg: Some(proto::sync_message::Msg::ManifestResponse(response)),
            })
            .unwrap();
            let frame = std::sync::Arc::new(frame);
            let expected = std::sync::Arc::new(expected);
            let local_hashes = std::sync::Arc::new(local_hashes);

            let running = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
            let heartbeat = if observe_heartbeat {
                let running = running.clone();
                Some(tokio::spawn(async move {
                    let mut gaps = Vec::new();
                    while running.load(std::sync::atomic::Ordering::Acquire) {
                        let started = std::time::Instant::now();
                        tokio::task::yield_now().await;
                        gaps.push(started.elapsed());
                    }
                    gaps
                }))
            } else {
                None
            };

            let mut worker_samples = Vec::with_capacity(20);
            let mut coordination_samples = Vec::with_capacity(20);
            for sample in 0..25 {
                let frame = frame.clone();
                let expected = expected.clone();
                let local_hashes = local_hashes.clone();
                let policy = policy.clone();
                let started = std::time::Instant::now();
                let (verified, worker_elapsed) = tokio::task::spawn_blocking(move || {
                    let worker_started = std::time::Instant::now();
                    let message = distd_core::utils::frame::decode_length_delimited::<
                        proto::SyncMessage,
                    >(&frame)
                    .unwrap();
                    let response = match message.msg {
                        Some(proto::sync_message::Msg::ManifestResponse(response)) => response,
                        _ => panic!("reference fixture must be a manifest response"),
                    };
                    let verified = verify_manifest_response(
                        response,
                        &expected,
                        local_hashes.as_ref().clone(),
                        &policy,
                    )
                    .unwrap();
                    (verified, worker_started.elapsed())
                })
                .await
                .unwrap();
                let total_elapsed = started.elapsed();
                tokio::task::spawn_blocking(move || drop(verified))
                    .await
                    .unwrap();
                if sample >= 5 {
                    worker_samples.push(worker_elapsed);
                    coordination_samples.push(total_elapsed.saturating_sub(worker_elapsed));
                }
            }

            running.store(false, std::sync::atomic::Ordering::Release);
            let heartbeat = match heartbeat {
                Some(heartbeat) => heartbeat.await.unwrap(),
                None => Vec::new(),
            };
            (worker_samples, coordination_samples, heartbeat)
        }

        let workloads = [
            (1, false, std::time::Duration::from_micros(250)),
            (1, true, std::time::Duration::from_micros(250)),
            (64, false, std::time::Duration::from_micros(250)),
            (64, true, std::time::Duration::from_micros(250)),
            (4_096, false, std::time::Duration::from_millis(2)),
            (4_096, true, std::time::Duration::from_millis(2)),
            (65_536, false, std::time::Duration::from_millis(20)),
            (65_536, true, std::time::Duration::from_millis(20)),
        ];
        let mut heartbeat = Vec::new();
        let mut coordination = Vec::new();
        let mut worker_results = Vec::new();
        for (chunk_count, directory, ceiling) in workloads {
            let (samples, observed_coordination, observed_heartbeat) =
                campaign(chunk_count, directory, chunk_count == 65_536 && directory).await;
            let p95 = percentile_95(&samples);
            let maximum = *samples.iter().max().unwrap();
            println!(
                "publisher_client_reference shape={} chunks={} raw_ns={:?} p95_ns={} max_ns={}",
                if directory { "directory" } else { "file" },
                chunk_count,
                samples
                    .iter()
                    .map(std::time::Duration::as_nanos)
                    .collect::<Vec<_>>(),
                p95.as_nanos(),
                maximum.as_nanos(),
            );
            worker_results.push((chunk_count, directory, p95, maximum, ceiling));
            heartbeat.extend(observed_heartbeat);
            coordination.extend(observed_coordination);
        }
        let coordination_p95 = percentile_95(&coordination);
        let coordination_p99 = percentile(&coordination, 99);
        let coordination_max = *coordination.iter().max().unwrap();
        println!(
            "publisher_client_reference coordination_raw_ns={:?} p95_ns={} p99_ns={} max_ns={}",
            coordination
                .iter()
                .map(std::time::Duration::as_nanos)
                .collect::<Vec<_>>(),
            coordination_p95.as_nanos(),
            coordination_p99.as_nanos(),
            coordination_max.as_nanos(),
        );
        assert!(coordination_p95 <= std::time::Duration::from_millis(1));
        assert!(coordination_p99 <= std::time::Duration::from_millis(5));
        let heartbeat_p95 = percentile_95(&heartbeat);
        let heartbeat_p99 = percentile(&heartbeat, 99);
        let heartbeat_max = *heartbeat.iter().max().unwrap();
        println!(
            "publisher_client_reference heartbeat_samples={} p95_ns={} p99_ns={} max_ns={}",
            heartbeat.len(),
            heartbeat_p95.as_nanos(),
            heartbeat_p99.as_nanos(),
            heartbeat_max.as_nanos(),
        );
        assert!(heartbeat_p95 <= std::time::Duration::from_millis(1));
        assert!(heartbeat_p99 <= std::time::Duration::from_millis(5));

        let configured = (0..crate::publisher::MAX_PUBLISHER_KEYS)
            .map(|index| format!("{index:064x}"))
            .collect::<Vec<_>>();
        let mut policy_samples = Vec::with_capacity(20);
        for sample in 0..25 {
            let started = std::time::Instant::now();
            let policy = PublisherPolicy::from_config(&configured, false).unwrap();
            let elapsed = started.elapsed();
            assert_eq!(policy.key_count(), crate::publisher::MAX_PUBLISHER_KEYS);
            if sample >= 5 {
                policy_samples.push(elapsed);
            }
        }
        let policy_p95 = percentile_95(&policy_samples);
        let policy_max = *policy_samples.iter().max().unwrap();
        println!(
            "publisher_client_reference policy_raw_ns={:?} p95_ns={} max_ns={}",
            policy_samples
                .iter()
                .map(std::time::Duration::as_nanos)
                .collect::<Vec<_>>(),
            policy_p95.as_nanos(),
            policy_max.as_nanos(),
        );
        assert!(policy_p95 <= std::time::Duration::from_millis(5));
        assert!(policy_max <= std::time::Duration::from_millis(10));

        let (_, metadata_item, _) = signed_manifest_fixture();
        let mut metadata = distd_core::metadata::Server::default();
        for index in 0..10_000 {
            let mut item = metadata_item.clone();
            item.artifact_id = format!("catalog-{index:05}");
            metadata.items.insert(item.artifact_id.clone(), item);
        }
        let metadata = tokio::sync::RwLock::new(std::sync::Arc::new(metadata));
        let mut lock_samples = Vec::with_capacity(100);
        for _ in 0..100 {
            let started = std::time::Instant::now();
            let selected = metadata
                .read()
                .await
                .items
                .get("catalog-09999")
                .cloned()
                .unwrap();
            lock_samples.push(started.elapsed());
            assert_eq!(selected.artifact_id, "catalog-09999");
        }
        let lock_p95 = percentile_95(&lock_samples);
        println!(
            "publisher_client_reference metadata_lock_raw_ns={:?} p95_ns={}",
            lock_samples
                .iter()
                .map(std::time::Duration::as_nanos)
                .collect::<Vec<_>>(),
            lock_p95.as_nanos(),
        );
        assert!(lock_p95 <= std::time::Duration::from_millis(1));

        for (chunk_count, directory, p95, maximum, ceiling) in worker_results {
            assert!(
                p95 <= ceiling,
                "{} {chunk_count} p95 {p95:?} > {ceiling:?}",
                if directory { "directory" } else { "file" }
            );
            if chunk_count == 65_536 {
                assert!(maximum <= std::time::Duration::from_millis(35));
            }
        }
    }

    #[test]
    fn activation_receipt_is_unforgeable_and_bound_to_exact_pending_identity() {
        let directory = tempfile::tempdir().unwrap();
        let mut persistent = ClientPersistentState::load_from_paths(
            directory.path().join("trust.state"),
            directory.path().join("legacy.json"),
        )
        .unwrap();
        let url = CanonicalServerUrl::parse("quic://server-a.example:443").unwrap();
        let record = ServerTrustRecord {
            certificate_pin: [7; 32],
            client_uuid: uuid::Uuid::from_u128(17),
        };
        // Receipt fields are private and no constructor exists. Successful
        // durable installation is the only safe way to obtain this capability.
        let receipt = persistent.install(url.clone(), record).unwrap();

        assert!(receipt_matches_pending(
            &receipt,
            &url,
            record.certificate_pin,
            record.client_uuid,
        ));
        let other_url = CanonicalServerUrl::parse("quic://server-b.example:443").unwrap();
        assert!(!receipt_matches_pending(
            &receipt,
            &other_url,
            record.certificate_pin,
            record.client_uuid,
        ));
        assert!(!receipt_matches_pending(
            &receipt,
            &url,
            [8; 32],
            record.client_uuid,
        ));
        assert!(!receipt_matches_pending(
            &receipt,
            &url,
            record.certificate_pin,
            uuid::Uuid::from_u128(18),
        ));
    }

    #[allow(dead_code)]
    fn pending_type_state_exposes_identity_but_no_server_operations(pending: &PendingServer) {
        let _ = pending.canonical_url();
        let _ = pending.observed_pin();
        let _ = pending.client_uuid();
        // Metadata, sync, reconnect, and fetch-loop operations exist only on
        // `Server`, obtained only by consuming `pending.activate(receipt)`.
        // `DurableTrustReceipt` fields are private and it has no constructor.
    }

    #[allow(dead_code)]
    fn active_identity_is_read_only_and_reconnect_bound(server: &super::Server) {
        let _: &CanonicalServerUrl = server.canonical_url();
        let _: [u8; 32] = server.certificate_pin();
        let _: uuid::Uuid = server.client_uuid();
        // The authority and pin fields are private. Reconnect reads those same
        // immutable fields together with this private enrolled UUID.
    }
}
