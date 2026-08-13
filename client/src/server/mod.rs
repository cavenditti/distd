use crate::error::ServerRequest;

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
#[derive(Debug, Clone)]
pub struct Server {
    //pub connection: ..

    // server address
    //pub addr: SocketAddr,
    /// server url
    pub url: String,

    /// server Ed25519 public key
    pub pub_key: [u8; 32], // TODO Check this

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

impl Server {
    /// Create a new server instance
    ///
    /// # Arguments
    /// * `url` - server url
    /// * `pub_key` - client public key, TODO
    /// * `client_name` - client name
    /// * `timeout` - timeout for server fetches, TODO
    ///
    /// # Returns
    /// A new server instance
    ///
    /// # Errors
    /// * `ServerRequest::BadPubKey` - if the public key is invalid
    /// * `ServerRequest::Request` - if the request fails
    /// * `ServerRequest::Utf8` - if the response is not valid utf8
    /// * `ServerRequest::Uuid` - if the response is not a valid uuid
    ///
    /// # Panics
    /// * If the url does not have a scheme or authority
    pub async fn new(
        url: &str,
        //pub_key: &PublicKey,
        client_name: &str,
        client_uuid: Option<Uuid>,
        expected_certificate: Option<[u8; 32]>,
        enrollment_token: Option<&str>,
    ) -> Result<Self, ServerRequest> {
        let transport = Self::make_transport(
            url,
            &client_uuid.unwrap_or_else(Uuid::nil),
            expected_certificate,
        )
        .await?;
        tracing::debug!("Connected to server");
        let certificate_fingerprint = transport.certificate_fingerprint();

        let timeout = Duration::new(5, 0); // TODO make this configurable

        let mut server = Self {
            pub_key: certificate_fingerprint,
            url: url.to_string(),
            client_uuid,
            client_name: client_name.to_string(),
            enrollment_token: enrollment_token.map(str::to_owned),
            shared: Arc::new(RwLock::new(SharedServer {
                metadata: ServerMetadata::default(),
                transport,
                last_update: Instant::now(),
            })),
            timeout,
        };
        server.register().await?;
        server.fetch().await?;

        Ok(server)
    }

    /// Get the client uuid
    #[must_use]
    pub fn client_uuid(&self) -> Uuid {
        self.client_uuid.unwrap_or(Uuid::nil())
    }

    async fn make_transport(
        url: &str,
        uuid: &Uuid,
        expected_certificate: Option<[u8; 32]>,
    ) -> Result<TransportClient, ServerRequest> {
        QuicTransportClient::connect(
            url,
            if uuid.is_nil() { None } else { Some(*uuid) },
            expected_certificate,
        )
        .await
        .map(TransportClient::Quic)
    }

    /// Register a new client
    pub async fn register(&mut self) -> Result<Uuid, ServerRequest> {
        let mut shared = self.shared.write().await;

        tracing::trace!("Starting `Register` request");
        let res = shared
            .transport
            .register(distd_core::proto::ClientRegister {
                name: self.client_name.to_string(),
                version: VERSION.to_string(),
                uuid: self.client_uuid.map(|uuid| uuid.as_bytes().to_vec()),
                enrollment_token: self.enrollment_token.clone(),
            })
            .await?;
        tracing::trace!("Parsed `Register` response");

        let uuid = res.uuid.ok_or(ServerRequest::MissingUuid)?;
        let uuid: [u8; 16] = uuid.try_into().map_err(|_| ServerRequest::BadUuid)?;
        let uuid = Uuid::from_bytes(uuid);
        tracing::info!("Got uuid '{uuid:?}' from server");

        self.client_uuid = Some(uuid);
        shared.transport.set_client_uuid(uuid).await;

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
                    Self::make_transport(&self.url, &self.client_uuid(), Some(self.pub_key)).await
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
