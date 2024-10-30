//use std::{net::SocketAddr
use crate::{error::ServerRequest, grpc::DistdGrpcClient};

use std::{fmt::Debug, sync::Arc, time::Duration};
use uuid::Uuid;

use tokio::{sync::RwLock, time::Instant};

//use ring::agreement::PublicKey;

use distd_core::{
    error::InvalidParameter,
    hash::Hash,
    metadata::Server as ServerMetadata,
    proto::{distd_client::DistdClient, Hashes, SerializedTree},
    tonic::{service::interceptor::InterceptedService, transport::Channel, Streaming},
    utils::grpc::uuid_to_metadata,
    version::VERSION,
    Request,
};

/// Shared server-related data to be kept behind an async lock
#[derive(Debug)]
struct SharedServer {
    /// global server metadata
    pub metadata: ServerMetadata,

    /// last time metadata was fetched from server
    pub last_update: Instant,

    /// client for gRPC requests to server
    pub grpc_client: distd_core::Client<InterceptedService<Channel, DistdGrpcClient>>,
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
        pub_key: &[u8; 32],
    ) -> Result<Self, ServerRequest> {
        let grpc_client = Self::make_grpc_client(url, &Uuid::nil()).await?;
        tracing::debug!("Connected to server");

        let timeout = Duration::new(5, 0); // TODO make this configurable

        let mut server = Self {
            pub_key: pub_key
                .as_ref()
                .try_into()
                .map_err(|_| ServerRequest::BadPubKey)?,
            url: url.to_string(),
            client_uuid,
            client_name: client_name.to_string(),
            shared: Arc::new(RwLock::new(SharedServer {
                metadata: ServerMetadata::default(),
                grpc_client,
                last_update: Instant::now(),
            })),
            timeout,
        };
        server.register().await?;
        server.fetch().await?;

        Ok(server)
    }

    /// Get the client uuid
    pub fn client_uuid(&self) -> Uuid {
        self.client_uuid.unwrap_or(Uuid::nil())
    }

    async fn make_grpc_client(
        url: &str,
        uuid: &Uuid,
    ) -> Result<DistdClient<InterceptedService<Channel, DistdGrpcClient>>, ServerRequest> {
        tracing::debug!("Connecting to server at {url}");
        let grpc_channel = distd_core::tonic::transport::Channel::from_shared(url.to_string())
            .map_err(InvalidParameter::Uri)?
            .connect()
            .await?;
        Ok(distd_core::Client::with_interceptor(
            grpc_channel,
            DistdGrpcClient {
                uuid: uuid_to_metadata(&uuid),
            },
        ))
    }

    /// Register a new client
    pub async fn register(&mut self) -> Result<Uuid, ServerRequest> {
        let mut shared = self.shared.write().await;

        tracing::trace!("Starting `Register` request");
        let res = shared
            .grpc_client
            .register(Request::new(distd_core::proto::ClientRegister {
                name: self.client_name.to_string(),
                version: VERSION.to_string(),
                uuid: self.client_uuid.map(|uuid| uuid.as_bytes().to_vec()),
            }))
            .await?
            .into_inner();
        tracing::trace!("Parsed `Register` response");

        let uuid = res.uuid.ok_or(ServerRequest::MissingUuid)?;
        let uuid: [u8; 16] = uuid.try_into().map_err(|_| ServerRequest::BadUuid)?;
        let uuid = Uuid::from_bytes(uuid);
        tracing::info!("Got uuid '{uuid:?}' from server");

        // Update client_uuid and create a new gRPC connection setting it in the metadata
        self.client_uuid = Some(uuid);
        shared.grpc_client = Self::make_grpc_client(&self.url, &self.client_uuid()).await?;

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
            .grpc_client
            .fetch(Request::new(distd_core::proto::ClientKeepAlive {}))
            .await?
            .into_inner();
        tracing::trace!("Parsed `Fetch` response");

        let new_metadata = bitcode::deserialize(&res.serialized)?;
        shared.last_update = Instant::now();

        if shared.metadata != new_metadata {
            shared.metadata = new_metadata;
            tracing::trace!("New metadata: {:?}", shared.metadata);
        }

        Ok(())
    }

    // TODO diff may optionally be computed client-side
    /// Transfer chunks from server, computing diff from local data
    pub async fn transfer_diff(
        &self,
        item_path: String,
        request_version: Option<u32>,
        from_version: Option<u32>,
        from: &[Hash],
    ) -> Result<Streaming<SerializedTree>, ServerRequest> {
        tracing::trace!("Preparing transfer/diff request: target: '{item_path}', {from_version:?}->{request_version:?}, {from:?}");
        let mut shared = self.shared.write().await;

        // comma separated list of hashes
        let from = from
            .into_iter()
            .map(|x| x.as_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();

        Ok(shared
            .grpc_client
            .tree_transfer(Request::new(distd_core::proto::ItemRequest {
                item_path,
                request_version,
                from_version,
                hashes: Some(Hashes { hashes: from }),
            }))
            .await?
            .into_inner())
    }

    /// Fetch metadata from server in a loop
    pub async fn fetch_loop(self) {
        loop {
            tokio::time::sleep(self.timeout).await;
            if self.fetch().await.is_err() {
                // try to re-establish connection to server
                if let Ok(client) = Self::make_grpc_client(&self.url, &self.client_uuid()).await {
                    tracing::info!("Connected to server");
                    self.shared.write().await.grpc_client = client;
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
