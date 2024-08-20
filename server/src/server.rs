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
use ring::signature::Ed25519KeyPair;
use ring::{
    rand,
    signature::{self},
};
use uuid::Uuid;

use crate::client::{Client, ClientName};
use crate::error::ServerError;
use distd_core::feed::{Feed, FeedName};
use distd_core::version::{Version, VERSION};

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
    pub global_metadata: RwLock<ServerMetadata>,
    // Feed map
    pub feeds: RwLock<HashMap<FeedName, Feed>>,
    // Client map
    pub clients: RwLock<BTreeMap<Uuid, Client>>,
    // A storage implementing ChunkStorage, basically a key-value database of some sort
    pub storage: T,
    // Map associating each item name with its metadata (including chunk hashes)
    pub item_map: RwLock<HashMap<ItemName, Item>>,
    // server version
    pub version: Version,
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
            global_metadata: RwLock::new(ServerMetadata::default()),
            feeds: RwLock::new(HashMap::<FeedName, Feed>::new()),
            clients: RwLock::new(BTreeMap::<Uuid, Client>::new()),
            storage: T::default(),
            item_map: RwLock::new(HashMap::<ItemName, Item>::new()),
            version: *VERSION,
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
        global_metadata: ServerMetadata,
        feeds: Vec<Feed>,
    ) -> Result<Self, KeyRejected> {
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref())?;
        Ok(Self {
            key_pair,
            uuid_nonce: blake3::hash(pkcs8_bytes.as_ref()).to_string(),
            global_metadata: RwLock::new(global_metadata),
            feeds: RwLock::new(HashMap::<FeedName, Feed>::from_iter(
                feeds.into_iter().map(|x| (x.name.clone(), x)),
            )),
            clients: RwLock::new(BTreeMap::<Uuid, Client>::new()), // TODO save and reload from disk
            storage: T::default(),
            item_map: RwLock::new(HashMap::<ItemName, Item>::new()),
            version: *VERSION,
        })
    }

    pub fn register_client(
        &self,
        name: ClientName,
        addr: SocketAddr,
        version: Option<Version>,
    ) -> Result<Uuid, RegisterError> {
        let nonced_name = name.clone() + &self.uuid_nonce;
        let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, &nonced_name.as_bytes());
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
        self.feeds
            .write()
            .ok()
            .and_then(|mut feeds| feeds.try_insert(feed.name.clone(), feed).ok().cloned())
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
        .map_err(|e| ServerError::ChunkInsertError(e))
        .and_then(|i| {
            self.item_map
                .write()
                .map_err(|_e| ServerError::LockError)?
                .try_insert(i.name.clone(), i)
                .map(|item| item.to_owned())
                .map_err(|e| ServerError::ItemInsertionError(e.entry.key().clone()))
        })
    }
}
