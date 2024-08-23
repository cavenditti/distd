use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::RwLock;
use std::time::SystemTime;

use axum::body::Bytes;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::item::{Item, ItemName};
use distd_core::metadata::ServerMetadata;
use ring::error::KeyRejected;
use ring::pkcs8::Document;
use ring::signature::{Ed25519KeyPair, KeyPair};
use ring::{
    rand,
    signature::{self},
};
use uuid::Uuid;

use crate::client::{Client, ClientName};
use crate::error::ServerError;
use distd_core::feed::{Feed, FeedName};
use distd_core::version::Version;

/// Data structure used internally by server, may be converted to ServerMetadata
#[derive(Debug, Clone, Default)]
pub struct ServerInternalMetadata {
    // TODO
    // server version
    pub version: Version,
    // Feed map
    pub feeds: HashMap<FeedName, Feed>,
    // Item map
    pub items: HashMap<ItemName, Item>,
}

impl From<ServerInternalMetadata> for ServerMetadata {
    fn from(value: ServerInternalMetadata) -> Self {
        Self {
            version: value.version,
            feeds: value.feeds,
            items: HashMap::from_iter(
                value
                    .items
                    .iter()
                    .map(|x| (x.0.clone(), x.1.metadata.clone())),
            ),
        }
    }
}

/// distd Server
///
/// Server signature is used to check replicated data among clients when shared p2p,
/// Note that this is different from an eventual "build" signature.
pub struct Server<T>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    key_pair: Ed25519KeyPair, // needs server restart to be changed
    uuid_nonce: String,       // needs server restart to be changed
    // global server metadata
    pub metadata: RwLock<ServerInternalMetadata>,
    // A storage implementing ChunkStorage, basically a key-value database of some sort
    pub storage: T,
    // Client map
    pub clients: RwLock<BTreeMap<Uuid, Client>>,
}

impl<T> Default for Server<T>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
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
            key_pair,
            uuid_nonce,
            metadata: RwLock::new(ServerInternalMetadata::default()),
            clients: RwLock::new(BTreeMap::<Uuid, Client>::new()),
            storage: T::default(),
        }
    }
}

#[derive(Debug)]
pub struct RegisterError;

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Clone + Default,
{
    pub fn new(
        pkcs8_bytes: Document,
        metadata: ServerInternalMetadata,
    ) -> Result<Self, KeyRejected> {
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref())?;
        Ok(Self {
            key_pair,
            uuid_nonce: blake3::hash(pkcs8_bytes.as_ref()).to_string(),
            metadata: RwLock::new(metadata),
            clients: RwLock::new(BTreeMap::<Uuid, Client>::new()), // TODO save and reload from disk
            storage: T::default(),
        })
    }

    pub fn register_client(
        &self,
        name: ClientName,
        addr: SocketAddr,
        version: Option<Version>,
    ) -> Result<Uuid, RegisterError> {
        let nonced_name = name.clone() + &self.uuid_nonce;
        let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, nonced_name.as_bytes());
        let client = Client {
            addr,
            name,
            uuid,
            version,
            last_heartbeat: SystemTime::now(),
        };
        self.clients
            .write()
            .ok()
            .and_then(|mut clients| clients.try_insert(client.uuid, client).cloned().ok())
            .map(|client| client.uuid)
            .ok_or(RegisterError)
    }

    pub fn expose_feed(&self, feed: Feed) -> Result<FeedName, RegisterError> {
        self.metadata
            .write()
            .ok()
            .and_then(|mut metadata| {
                metadata
                    .feeds
                    .try_insert(feed.name.clone(), feed)
                    .ok()
                    .cloned()
            })
            .map(|feed| feed.name.clone())
            .ok_or(RegisterError)
    }

    pub fn publish_item(
        &self,
        name: ItemName,
        path: PathBuf,
        revision: u32,
        description: Option<String>,
        file: Bytes,
    ) -> Result<Item, ServerError> {
        Item::new(
            name,
            path,
            revision,
            description,
            file,
            self.storage.clone(),
        )
        .map_err(ServerError::ChunkInsertError)
        .and_then(|i| {
            self.metadata
                .write()
                .map_err(|_e| ServerError::LockError)?
                .items
                .try_insert(i.metadata.name.clone(), i)
                .map(|item| item.to_owned())
                .map_err(|e| ServerError::ItemInsertionError(e.entry.key().clone()))
        })
    }

    pub fn get_public_key(&self) -> &[u8] {
        self.key_pair.public_key().as_ref()
    }
}
