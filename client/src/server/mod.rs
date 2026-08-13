use crate::{
    error::ServerRequest,
    persistence::{DurableTrustReceipt, ServerTrustRecord},
    server_url::CanonicalServerUrl,
};

use std::collections::HashSet;
use std::{fmt::Debug, sync::Arc, time::Duration};
use uuid::Uuid;

use tokio::{sync::RwLock, time::Instant};

//use ring::agreement::PublicKey;

use distd_core::{
    chunk_storage::ReceivedChunk,
    chunks::{ChunkAlgorithm, ChunkInfo},
    hash::Hash,
    item::{FileEntry, Manifest},
    metadata::Server as ServerMetadata,
    proto::{self},
    version::VERSION,
};

mod quic;

use quic::{QuicSyncSession, QuicTransportClient};

#[derive(Debug)]
enum TransportClient {
    Quic(QuicTransportClient),
}

#[derive(Debug)]
enum SyncContinuation {
    Quic(QuicSyncSession),
}

impl TransportClient {
    fn certificate_fingerprint(&self) -> [u8; 32] {
        match self {
            Self::Quic(client) => client.certificate_fingerprint(),
        }
    }

    async fn register(
        &mut self,
        request: distd_core::proto::ClientRegister,
    ) -> Result<distd_core::proto::ServerMetadata, ServerRequest> {
        match self {
            Self::Quic(client) => client.register(request).await,
        }
    }

    async fn fetch(
        &mut self,
        request: distd_core::proto::ClientKeepAlive,
    ) -> Result<distd_core::proto::ServerMetadata, ServerRequest> {
        match self {
            Self::Quic(client) => client.fetch(request).await,
        }
    }

    async fn set_client_uuid(&mut self, client_uuid: Uuid) {
        match self {
            Self::Quic(client) => client.set_client_uuid(client_uuid).await,
        }
    }

    async fn open_sync(
        &mut self,
        manifest_request: proto::ManifestRequest,
    ) -> Result<(proto::ManifestResponse, SyncContinuation), ServerRequest> {
        match self {
            Self::Quic(client) => {
                let (manifest, session) = client.sync(manifest_request).await?;
                Ok((manifest, SyncContinuation::Quic(session)))
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
        }
    }
}

/// Shared server-related data to be kept behind an async lock
#[derive(Debug)]
struct SharedServer {
    /// global server metadata
    pub metadata: ServerMetadata,

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

    /// server Ed25519 public key
    pub_key: [u8; 32], // TODO Check this

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
            .field("pub_key", &self.pub_key)
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
        let mut inner = self
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
        let metadata = match fetch_metadata(&mut inner.transport).await {
            Ok(metadata) => metadata,
            Err(error) => {
                inner.transport.close();
                return Err(error);
            }
        };

        Ok(Server {
            pub_key: inner.certificate_fingerprint,
            url: inner.url,
            client_uuid: Some(inner.client_uuid),
            client_name: inner.client_name,
            enrollment_token: inner.enrollment_token,
            shared: Arc::new(RwLock::new(SharedServer {
                metadata,
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
        self.pub_key
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
        let mut shared = self.shared.write().await;
        let uuid = register_transport(
            &mut shared.transport,
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
        self.shared.read().await.metadata.clone()
    }

    /// Get the last time metadata was fetched from the server
    pub async fn last_update(&self) -> Instant {
        self.shared.read().await.last_update
    }

    /// Fetch metadata from server
    async fn fetch(&self) -> Result<(), ServerRequest> {
        tracing::trace!("Starting `Fetch` request");

        let mut shared = self.shared.write().await;

        assert!(self.client_uuid.is_some());

        //distd_core::AcknowledgeRequest::new(distd_core::proto::EnumAcknowledge::AckOk);
        let res = shared
            .transport
            .fetch(distd_core::proto::ClientKeepAlive {})
            .await?;
        tracing::trace!("Parsed `Fetch` response");

        let new_metadata = bitcode::deserialize(&res.serialized)?;
        shared.last_update = Instant::now();

        if shared.metadata != new_metadata {
            shared.metadata = new_metadata;
            tracing::trace!("New metadata: {:?}", shared.metadata);
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
        let (manifest, chunk_infos, received, wire_bytes) =
            self.sync_artifact_shared(artifact_id, local_hashes).await?;
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
        let mut shared = self.shared.write().await;
        let manifest_request = proto::ManifestRequest {
            artifact_id: artifact_id.to_string(),
            version: None,
        };
        let (manifest_resp, continuation) = shared.transport.open_sync(manifest_request).await?;

        let manifest = Manifest {
            artifact_id: manifest_resp.artifact_id,
            version: manifest_resp.version,
            root_hash: Hash::from_bytes(
                manifest_resp
                    .root_hash
                    .try_into()
                    .map_err(|_| ServerRequest::BadHash)?,
            ),
            total_size: manifest_resp.total_size,
            chunk_count: manifest_resp.chunk_count,
            chunk_size: manifest_resp.chunk_size,
            chunk_algorithm: ChunkAlgorithm::from_proto(manifest_resp.chunk_algorithm.as_ref()),
            entries: manifest_resp
                .entries
                .into_iter()
                .map(|entry| FileEntry {
                    relative_path: entry.relative_path,
                    size: entry.size,
                    chunk_range: (entry.chunk_start, entry.chunk_end),
                })
                .collect(),
        };

        let chunk_hashes: Vec<Hash> = manifest_resp
            .chunk_hashes
            .into_iter()
            .map(|b| -> Result<Hash, ServerRequest> {
                let arr: [u8; 32] = b.try_into().map_err(|_| ServerRequest::BadHash)?;
                Ok(Hash::from_bytes(arr))
            })
            .collect::<Result<Vec<_>, _>>()?;

        if manifest_resp.chunk_sizes.len() != chunk_hashes.len() {
            return Err(ServerRequest::UnexpectedMessage);
        }

        let chunk_infos: Vec<ChunkInfo> = chunk_hashes
            .into_iter()
            .zip(manifest_resp.chunk_sizes.into_iter())
            .map(|(hash, size)| ChunkInfo {
                hash,
                size: size as u64,
            })
            .collect();

        let available_hashes: HashSet<Hash> = local_hashes.iter().copied().collect();
        let mut local_bitfield = distd_core::possession::Bitfield::empty(manifest.chunk_count);
        for (index, chunk_info) in chunk_infos.iter().enumerate() {
            if available_hashes.contains(&chunk_info.hash) {
                local_bitfield.set(index as u32);
            }
        }

        let chunk_sizes: Vec<usize> = chunk_infos
            .iter()
            .map(|chunk| chunk.size as usize)
            .collect();

        let (received, wire_bytes) = continuation
            .send_possession_and_collect_chunks_shared(
                proto::PossessionBitfield {
                    bitfield: local_bitfield.to_bytes(),
                },
                &chunk_sizes,
            )
            .await?;

        Ok((manifest, chunk_infos, received, wire_bytes))
    }

    /// Fetch metadata from server in a loop
    pub async fn fetch_loop(mut self) {
        loop {
            tokio::time::sleep(self.timeout).await;
            if self.fetch().await.is_err() {
                // try to re-establish connection to server
                if let Ok(client) =
                    Self::make_transport(&self.url, Some(self.client_uuid()), Some(self.pub_key))
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

async fn register_transport(
    transport: &mut TransportClient,
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

async fn fetch_metadata(transport: &mut TransportClient) -> Result<ServerMetadata, ServerRequest> {
    tracing::trace!("Starting `Fetch` request");
    let response = transport
        .fetch(distd_core::proto::ClientKeepAlive {})
        .await?;
    tracing::trace!("Parsed `Fetch` response");
    bitcode::deserialize(&response.serialized).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::{
        assigned_uuid, receipt_matches_pending, redacted_token, require_same_prior_identity,
        PendingServer,
    };
    use crate::error::ServerRequest;
    use crate::{
        persistence::{ClientPersistentState, ServerTrustRecord},
        server_url::CanonicalServerUrl,
    };

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
