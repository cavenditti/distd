use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;

use axum::body::Bytes;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::item::{ArtifactId, Item, Name as ItemName};
use distd_core::metadata::Server as ServerMetadata;
use distd_core::possession::Bitfield;
use distd_core::proto::{self, sync_message::Msg, ClientKeepAlive, ClientRegister, ServerMetadata as ProtoServerMetadata, SyncMessage};
use distd_core::utils::grpc::uuid_to_metadata;
use distd_core::utils::uuid::slice_to_uuid;
use distd_core::utils::serde::BitcodeSerializable;
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
use distd_core::hash::{hash as do_hash, Hash};
use distd_core::version::Version;

/// Data structure used internally by server, may be converted to `ServerMetadata`
#[derive(Debug, Clone, Default)]
pub struct InternalMetadata {
    // TODO
    // server version
    pub version: Version,
    // Feed map
    pub feeds: HashMap<FeedName, Feed>,
    // Item map — keyed by ArtifactId
    pub items: HashMap<ArtifactId, Item>,
}

impl From<InternalMetadata> for ServerMetadata {
    fn from(value: InternalMetadata) -> Self {
        Self {
            version: value.version,
            feeds: value.feeds,
            items: value
                .items
                .into_iter()
                .map(|(artifact_id, item)| (artifact_id, item.metadata))
                .collect(),
        }
    }
}

/// distd Server
///
/// Server signature is used to check replicated data among clients when shared p2p,
/// Note that this is different from an eventual "build" signature.
#[derive(Debug)]
pub struct Server<T>
where
    T: ChunkStorage + Sync + Send,
{
    key_pair: Arc<Ed25519KeyPair>, // needs server restart to be changed
    uuid_nonce: String,            // needs server restart to be changed

    /// global server metadata
    pub metadata: Arc<RwLock<InternalMetadata>>,
    /// A storage implementing `ChunkStorage`; owns its own concurrency internally.
    pub storage: Arc<T>,
    /// Client map
    pub clients: Arc<RwLock<BTreeMap<Uuid, Client>>>,

    /// gRPC interceptor for uuids check
    pub uuid_interceptor: UuidAuthInterceptor,
}

impl<T> Clone for Server<T>
where
    T: ChunkStorage + Sync + Send,
{
    fn clone(&self) -> Self {
        Self {
            key_pair: Arc::clone(&self.key_pair),
            uuid_nonce: self.uuid_nonce.clone(),
            metadata: Arc::clone(&self.metadata),
            storage: Arc::clone(&self.storage),
            clients: Arc::clone(&self.clients),
            uuid_interceptor: self.uuid_interceptor.clone(),
        }
    }
}

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Debug,
{
    /// Create a new server instance with a loaded key pair, initial metadata and storage.
    ///
    /// The `uuid_nonce` is derived from a random UUID (not from the key bytes) so that it
    /// remains unique across restarts even when the same key is reused.
    pub fn new(pkcs8_bytes: &Document, storage: T, metadata: InternalMetadata) -> Result<Self, KeyRejected> {
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref())?;
        // Use a cryptographically random nonce – never derived from key material.
        let uuid_nonce = Uuid::new_v4().to_string();
        Ok(Self {
            key_pair: Arc::new(key_pair),
            uuid_nonce,
            metadata: Arc::new(RwLock::new(metadata)),
            storage: Arc::new(storage),
            clients: Arc::new(RwLock::new(BTreeMap::new())),
            uuid_interceptor: UuidAuthInterceptor::default(),
        })
    }

    /// Create a server with a freshly generated ephemeral key pair and a default storage.
    ///
    /// **Not recommended for production.** In production use [`Server::new`] and persist the key
    /// on disk so that the server identity is stable across restarts.
    pub fn new_ephemeral(storage: T) -> Self {
        let rng = rand::SystemRandom::new();
        let pkcs8_bytes = signature::Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let key_pair = signature::Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref()).unwrap();
        Self {
            key_pair: Arc::new(key_pair),
            uuid_nonce: Uuid::new_v4().to_string(),
            metadata: Arc::new(RwLock::new(InternalMetadata::default())),
            storage: Arc::new(storage),
            clients: Arc::new(RwLock::new(BTreeMap::new())),
            uuid_interceptor: UuidAuthInterceptor::default(),
        }
    }
}

#[derive(Debug)]
pub struct RegisterError;

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Debug,
{
    pub async fn authenticate_client_uuid(&self, uuid: Option<Uuid>) -> Result<Uuid, String> {
        let uuid = uuid.ok_or_else(|| String::from("Unauthenticated"))?;
        if uuid.is_nil() || !self.clients.read().await.contains_key(&uuid) {
            return Err(String::from("Unauthenticated"));
        }
        Ok(uuid)
    }

    pub async fn register_response(
        &self,
        addr: SocketAddr,
        request: ClientRegister,
    ) -> Result<ProtoServerMetadata, String> {
        let uuid = self
            .register_client(
                request.name,
                addr,
                Version::from_str(&request.version).ok(),
                request.uuid.map(|x| slice_to_uuid(&x)),
            )
            .await
            .map_err(|_| String::from("Cannot assign new UUID"))?;
        let serialized = ServerMetadata::from(self.metadata.read().await.clone())
            .to_bitcode()
            .map_err(|_| String::from("Cannot serialize server metadata"))?;
        Ok(ProtoServerMetadata {
            serialized,
            uuid: Some(uuid.as_bytes().to_vec()),
        })
    }

    pub async fn fetch_response(
        &self,
        _request: ClientKeepAlive,
    ) -> Result<ProtoServerMetadata, String> {
        let serialized = ServerMetadata::from(self.metadata.read().await.clone())
            .to_bitcode()
            .map_err(|_| String::from("Cannot serialize server metadata"))?;
        Ok(ProtoServerMetadata {
            serialized,
            uuid: None,
        })
    }

    pub async fn sync_manifest_response(
        &self,
        manifest_req: proto::ManifestRequest,
    ) -> Result<(proto::ManifestResponse, Hash, usize), String> {
        let item = {
            let md = self.metadata.read().await;
            md.items.get(&manifest_req.artifact_id).cloned()
        }
        .ok_or_else(|| format!("Unknown artifact: {}", manifest_req.artifact_id))?;

        let root_hash = *item.root();
        let chunk_hashes = self.storage.chunk_list(&root_hash);
        let chunk_count = chunk_hashes.len();

        Ok((
            proto::ManifestResponse {
                artifact_id: item.manifest.artifact_id.clone(),
                version: item.manifest.version,
                root_hash: root_hash.as_bytes().to_vec(),
                total_size: item.manifest.total_size,
                chunk_count: item.manifest.chunk_count,
                chunk_size: item.manifest.chunk_size,
                chunk_hashes: chunk_hashes.iter().map(|h| h.as_bytes().to_vec()).collect(),
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
            },
            root_hash,
            chunk_count,
        ))
    }

    pub async fn sync_chunk_response_messages(
        &self,
        root_hash: Hash,
        expected_chunk_count: usize,
        possession: proto::PossessionBitfield,
    ) -> Result<Vec<SyncMessage>, String> {
        let bitfield = Bitfield::from_bytes(&possession.bitfield)
            .ok_or_else(|| String::from("Invalid bitfield"))?;
        if bitfield.chunk_count() as usize != expected_chunk_count {
            return Err(String::from("Invalid bitfield"));
        }

        let mut messages = Vec::new();

        let missing = bitfield.missing_indices();
        tracing::debug!(
            "Sync: {} missing chunks out of {}",
            missing.len(),
            bitfield.chunk_count()
        );

        if bitfield.is_empty() && !missing.is_empty() {
            const BULK_TARGET_BYTES: usize = 16 * 1024 * 1024;
            const BULK_MAX_CHUNKS: usize = 512;

            let mut cursor = 0;
            while cursor < missing.len() {
                let start_index = missing[cursor];
                let mut count = 0usize;
                let mut bulk_buf = Vec::with_capacity(BULK_TARGET_BYTES);

                while cursor < missing.len() && count < BULK_MAX_CHUNKS {
                    let idx = missing[cursor];
                    if idx != start_index + count as u32 {
                        break;
                    }

                    let Some(data) = self.storage.get_chunk_by_index(&root_hash, idx) else {
                        cursor += 1;
                        continue;
                    };

                    if count > 0 && bulk_buf.len() + data.len() > BULK_TARGET_BYTES {
                        break;
                    }

                    bulk_buf.extend_from_slice(&data);
                    count += 1;
                    cursor += 1;
                }

                if count == 0 {
                    continue;
                }

                messages.push(SyncMessage {
                    msg: Some(Msg::BulkData(proto::BulkData {
                        data: bulk_buf,
                        start_index,
                        count: count as u32,
                    })),
                });
            }
        } else {
            for idx in missing {
                if let Some(data) = self.storage.get_chunk_by_index(&root_hash, idx) {
                    messages.push(SyncMessage {
                        msg: Some(Msg::ChunkData(proto::ChunkData {
                            chunk_index: idx,
                            data,
                        })),
                    });
                }
            }
        }

        Ok(messages)
    }

    pub async fn sync_response_messages(
        &self,
        manifest_req: proto::ManifestRequest,
        possession: proto::PossessionBitfield,
    ) -> Result<Vec<SyncMessage>, String> {
        let (manifest, root_hash, expected_chunk_count) = self.sync_manifest_response(manifest_req).await?;
        let mut messages = Vec::with_capacity(1);
        messages.push(SyncMessage {
            msg: Some(Msg::ManifestResponse(manifest)),
        });
        messages.extend(
            self.sync_chunk_response_messages(root_hash, expected_chunk_count, possession)
                .await?,
        );
        Ok(messages)
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
        uuid: Option<Uuid>,
    ) -> Result<Uuid, RegisterError> {
        // tracing span
        let span = span!(tracing::Level::INFO, "register_client");
        let _entered = span.enter();

        tracing::info!("Got new client: \"{}\" ver:{:?}, @{}, {:?}", name, version, addr, uuid);
        let nonced_name = name.clone() + &self.uuid_nonce + &addr.to_string();
        tracing::debug!("Client nonced name: '{}'", nonced_name);

        if let Some(u) = uuid {
            if self.clients.read().await.contains_key(&u) {
                tracing::info!(
                    "Got existing uuid '{}' from \"{}\"@{}",
                    u.to_string(),
                    name,
                    addr
                );
                Ok(u)
            } else {
                tracing::warn!(
                    "Client reported invalid uuid '{}' from \"{}\"@{}",
                    u.to_string(),
                    name,
                    addr
                );
                Err(RegisterError)
            }
        } else {
            let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, nonced_name.as_bytes());
            tracing::info!(
                "Assigned new client uuid '{}' to \"{}\"@{}",
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

            let mut clients = self.clients.write().await;
            let uuid = client.uuid;
            match clients.entry(uuid) {
                std::collections::btree_map::Entry::Vacant(e) => {
                    e.insert(client);
                    Ok(uuid)
                }
                std::collections::btree_map::Entry::Occupied(e) => {
                    tracing::warn!("Client {} already registered", e.key());
                    Err(RegisterError)
                }
            }
        }
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn expose_feed(&self, feed: Feed) -> Result<FeedName, RegisterError> {
        let mut metadata = self.metadata.write().await;
        let name = feed.name.clone();
        match metadata.feeds.entry(name.clone()) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(feed);
                Ok(name)
            }
            std::collections::hash_map::Entry::Occupied(_) => Err(RegisterError),
        }
    }

    /// Publish a new item
    ///
    /// This function will insert the item into the storage and the metadata map.
    /// The item will be inserted into the metadata map using the artifact_id as key.
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
        // Derive artifact_id from name (matches Item::new default)
        let artifact_id: ArtifactId = name.clone();

        // Get last revision, if any. 0 otherwise
        let revision = self
            .metadata
            .read()
            .await
            .items
            .get(&artifact_id)
            .map(|i| i.metadata.revision + 1)
            .unwrap_or_default();

        // Check if already exists and if so just return the old one
        let root = &do_hash(&file);
        if let Some(old) = self.metadata.read().await.items.get(&artifact_id) {
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
            .create_item(name, path, revision, description, file)
            .map_err(|e| {
                tracing::error!("Storage error: {e}");
                ServerError::ChunkInsertError
            })?;

        self.metadata
            .write()
            .await
            .items
            .insert(artifact_id, item.clone());

        Ok(item)
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn publish_item_from_files(
        &self,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        mut files: Vec<(PathBuf, Bytes)>,
    ) -> Result<Item, ServerError> {
        let artifact_id: ArtifactId = name.clone();
        let revision = self
            .metadata
            .read()
            .await
            .items
            .get(&artifact_id)
            .map(|i| i.metadata.revision + 1)
            .unwrap_or_default();

        files.sort_by(|left, right| left.0.cmp(&right.0));

        let item = self
            .storage
            .create_item_from_files(name, path, revision, description, files)
            .map_err(|e| {
                tracing::error!("Storage error: {e}");
                ServerError::ChunkInsertError
            })?;

        self.metadata
            .write()
            .await
            .items
            .insert(artifact_id, item.clone());

        Ok(item)
    }

    /// Get the public key of the server
    #[must_use] pub fn public_key(&self) -> &[u8] {
        self.key_pair.public_key().as_ref()
    }
}
