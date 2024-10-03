use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use axum::body::Bytes;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::item::{Item, Name as ItemName};
use distd_core::metadata::Server as ServerMetadata;
use distd_core::utils::grpc::uuid_to_metadata;
use ring::error::KeyRejected;
use ring::pkcs8::Document;
use ring::signature::{Ed25519KeyPair, KeyPair};
use ring::{
    rand,
    signature::{self},
};
use tokio::sync::RwLock;
use tracing::span;
use uuid::Uuid;

use crate::client::{Client, Name as ClientName};
use crate::error::Server as ServerError;
use crate::grpc::UuidAuthInterceptor;
use distd_core::feed::{Feed, Name as FeedName};
use distd_core::hash::hash as do_hash;
use distd_core::version::Version;

/// Data structure used internally by server, may be converted to `ServerMetadata`
#[derive(Debug, Clone, Default)]
pub struct InternalMetadata {
    // TODO
    // server version
    pub version: Version,
    // Feed map
    pub feeds: HashMap<FeedName, Feed>,
    // Item map
    pub items: HashMap<PathBuf, Item>,
}

impl From<InternalMetadata> for ServerMetadata {
    fn from(value: InternalMetadata) -> Self {
        Self {
            version: value.version,
            feeds: value.feeds,
            items: value
                .items
                .iter()
                .map(|x| (x.0.clone(), x.1.metadata.clone()))
                .collect(),
        }
    }
}

/// distd Server
///
/// Server signature is used to check replicated data among clients when shared p2p,
/// Note that this is different from an eventual "build" signature.
#[derive(Debug, Clone)]
pub struct Server<T>
where
    T: ChunkStorage + Sync + Send + Default,
{
    key_pair: Arc<Ed25519KeyPair>, // needs server restart to be changed
    uuid_nonce: String,            // needs server restart to be changed

    /// global server metadata
    pub metadata: Arc<RwLock<InternalMetadata>>,
    /// A storage implementing ChunkStorage, basically a key-value database of some sort
    pub storage: Arc<RwLock<T>>,
    /// Client map
    pub clients: Arc<RwLock<BTreeMap<Uuid, Client>>>,

    /// gRPC interceptor for uuids check
    pub uuid_interceptor: UuidAuthInterceptor,
}

impl<T> Default for Server<T>
where
    T: ChunkStorage + Sync + Send + Default,
{
    fn default() -> Self {
        // Generate a key pair in PKCS#8 (v2) format.
        let rng = rand::SystemRandom::new();
        let pkcs8_bytes = signature::Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let uuid_nonce = blake3::hash(pkcs8_bytes.as_ref()).to_string();

        // Normally the application would store the PKCS#8 file persistently. Later
        // it would read the PKCS#8 file from persistent storage to use it.
        let key_pair = signature::Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref()).unwrap();

        Self {
            key_pair: Arc::new(key_pair),
            uuid_nonce,
            metadata: Arc::new(RwLock::new(InternalMetadata::default())),
            clients: Arc::new(RwLock::new(BTreeMap::<Uuid, Client>::new())),
            storage: Arc::default(),
            uuid_interceptor: UuidAuthInterceptor::default(),
        }
    }
}

#[derive(Debug)]
pub struct RegisterError;

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Default + Debug,
{
    /// Create a new server instance, with a specific key pair and metadata
    pub fn new(pkcs8_bytes: &Document, metadata: InternalMetadata) -> Result<Self, KeyRejected> {
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref())?;
        Ok(Self {
            key_pair: Arc::new(key_pair),
            uuid_nonce: blake3::hash(pkcs8_bytes.as_ref()).to_string(),
            metadata: Arc::new(RwLock::new(metadata)),
            ..Default::default()
        })
    }

    /// Register a new client
    ///
    /// This function will insert a new client into the clients map.
    /// The clients map key will be a UUID generated from the client name, the server nonce and the client address.
    #[allow(clippy::missing_panics_doc)]
    pub async fn register_client(
        &self,
        name: ClientName,
        addr: SocketAddr,
        version: Option<Version>,
    ) -> Result<Uuid, RegisterError> {
        // tracing span
        let span = span!(tracing::Level::INFO, "register_client");
        let _entered = span.enter();

        tracing::info!("Got new client: \"{}\" ver:{:?}, @{}", name, version, addr);
        let nonced_name = name.clone() + &self.uuid_nonce + &addr.to_string();
        tracing::debug!("Client nonced name: '{}'", nonced_name);
        let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, nonced_name.as_bytes());
        tracing::info!(
            "Assigned client uuid '{}' to \"{}\"@{}",
            uuid.to_string(),
            name,
            addr
        );
        let client = Client {
            addr,
            name,
            uuid,
            version,
            last_heartbeat: SystemTime::now(),
        };

        // Add uuid to valid list in interceptor
        self.uuid_interceptor
            .uuids
            .write()
            .unwrap()
            .insert(uuid_to_metadata(&uuid));

        self.clients
            .write()
            .await
            .try_insert(client.uuid, client)
            .inspect_err(|e| tracing::warn!("{}", e))
            .cloned()
            .ok()
            .map(|client| client.uuid)
            .ok_or(RegisterError)
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn expose_feed(&self, feed: Feed) -> Result<FeedName, RegisterError> {
        self.metadata
            .write()
            .await
            .feeds
            .try_insert(feed.name.clone(), feed)
            .ok()
            .cloned()
            .map(|feed| feed.name.clone())
            .ok_or(RegisterError)
    }

    /// Publish a new item
    ///
    /// This function will insert the item into the storage and the metadata map.
    /// The item will be inserted into the metadata map using the path as key.
    ///
    /// # Panics
    ///
    /// Conversion of paths to UTF-8 may panic on some OSes (Windows for sure)
    #[allow(clippy::missing_panics_doc)]
    pub async fn publish_item(
        &self,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        file: Bytes,
    ) -> Result<Item, ServerError> {
        // Get last revision, if any. 0 otherwise
        let revision = self
            .metadata
            .read()
            .await
            .items
            .get(&path)
            .map(|i| i.metadata.revision + 1)
            .unwrap_or_default();

        // Check if already exists and if so just return the old one
        // This is doing duplicated hashing calculations, may be improved
        let root = &do_hash(&file);
        if let Some(old) = self.metadata.read().await.items.get(&path) {
            if old.metadata.name == name
                && old.metadata.path == path
                && old.metadata.description == description
                && &old.metadata.root.hash == root
            {
                return Ok(old.clone());
            }
        }

        // Create item and return it
        let item = self
            .storage
            .write()
            .await
            .create_item(name, path, revision, description, file)
            .ok_or(ServerError::ChunkInsertError)?;

        self.metadata
            .write()
            .await
            .items
            .insert(item.metadata.path.clone(), item.clone());

        Ok(item)
    }

    /// Get the public key of the server
    pub fn public_key(&self) -> &[u8] {
        self.key_pair.public_key().as_ref()
    }
}
