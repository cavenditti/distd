use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::SystemTime;

use axum::body::Bytes;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::chunks::{chunk_algorithm_likely_precompressed, ChunkAlgorithm};
use distd_core::hash::Hash;
use distd_core::item::{ArtifactId, Item, Name as ItemName};
use distd_core::metadata::Server as ServerMetadata;
use distd_core::possession::Bitfield;
use distd_core::proto::{
    self, sync_message::Msg, ClientKeepAlive, ClientRegister,
    ServerMetadata as ProtoServerMetadata, SyncMessage,
};
use distd_core::transport::maybe_compress_sync_payload_owned;
use distd_core::utils::serde::BitcodeSerializable;
use distd_core::utils::uuid::slice_to_uuid;
use ring::error::KeyRejected;
use ring::pkcs8::Document;
use ring::signature::{Ed25519KeyPair, KeyPair};
use ring::{
    rand,
    signature::{self},
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, RwLock};
use tracing::span;
use uuid::Uuid;

use crate::client::{Client, Name as ClientName};
use crate::error::Server as ServerError;
use crate::metadata_state::MetadataStateStore;
use distd_core::feed::{Feed, Name as FeedName};
use distd_core::version::Version;

/// Data structure used internally by server, may be converted to `ServerMetadata`
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct InternalMetadata {
    // TODO
    // server version
    pub version: Version,
    // Feed map
    pub feeds: HashMap<FeedName, Feed>,
    // Item map — keyed by ArtifactId
    pub items: HashMap<ArtifactId, Item>,
    #[serde(default)]
    pub versions: HashMap<ArtifactId, BTreeMap<u64, Item>>,
}

impl InternalMetadata {
    pub fn verify_storage<T>(&self, storage: &T) -> Result<(), ServerError>
    where
        T: ChunkStorage,
    {
        crate::metadata_state::validate_runtime_metadata(self)?;
        for item in self.published_revisions() {
            storage.verify_committed_revision(&item).map_err(|error| {
                ServerError::State(format!(
                    "artifact {} version {} is not available in selected storage: {error}",
                    item.metadata.artifact_id, item.manifest.version
                ))
            })?;
        }
        for feed in self.feeds.values() {
            for item in feed.paths.values() {
                storage.verify_committed_revision(item).map_err(|error| {
                    ServerError::State(format!(
                        "feed {} item {} is not available in selected storage: {error}",
                        feed.name, item.metadata.artifact_id
                    ))
                })?;
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn published_revisions(&self) -> Vec<Item> {
        self.versions
            .values()
            .flat_map(|versions| versions.values().cloned())
            .collect()
    }
}

fn validate_item_identity(artifact_id: &str, item: &Item) -> Result<(), ServerError> {
    if item.metadata.artifact_id != artifact_id
        || item.manifest.artifact_id != artifact_id
        || item.manifest.version != u64::from(item.metadata.revision)
    {
        return Err(ServerError::State(format!(
            "artifact {artifact_id} metadata identity is inconsistent"
        )));
    }
    Ok(())
}

fn same_version_relevant_metadata(left: &Item, right: &Item) -> bool {
    left.metadata.artifact_id == right.metadata.artifact_id
        && left.metadata.name == right.metadata.name
        && left.metadata.path == right.metadata.path
        && left.metadata.description == right.metadata.description
        && left.metadata.root == right.metadata.root
        && left.manifest.entries == right.manifest.entries
        && left.manifest.chunk_algorithm == right.manifest.chunk_algorithm
        && left.chunks == right.chunks
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
    enrollment_token: Option<Arc<str>>,

    /// global server metadata
    metadata: Arc<RwLock<InternalMetadata>>,
    /// A storage implementing `ChunkStorage`; owns its own concurrency internally.
    storage: Arc<T>,
    /// Client map
    pub clients: Arc<RwLock<BTreeMap<Uuid, Client>>>,
    publish_locks: Arc<AsyncMutex<HashMap<ArtifactId, Arc<AsyncMutex<()>>>>>,
    metadata_persist_lock: Arc<StdMutex<()>>,
    metadata_store: Option<Arc<MetadataStateStore>>,
}

impl<T> Clone for Server<T>
where
    T: ChunkStorage + Sync + Send,
{
    fn clone(&self) -> Self {
        Self {
            key_pair: Arc::clone(&self.key_pair),
            uuid_nonce: self.uuid_nonce.clone(),
            enrollment_token: self.enrollment_token.clone(),
            metadata: Arc::clone(&self.metadata),
            storage: Arc::clone(&self.storage),
            clients: Arc::clone(&self.clients),
            publish_locks: Arc::clone(&self.publish_locks),
            metadata_persist_lock: Arc::clone(&self.metadata_persist_lock),
            metadata_store: self.metadata_store.clone(),
        }
    }
}

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send,
{
    pub(crate) async fn metadata_snapshot(&self) -> InternalMetadata {
        self.metadata.read().await.clone()
    }

    pub(crate) fn storage(&self) -> &T {
        &self.storage
    }

    #[cfg(test)]
    pub(crate) async fn lock_metadata_for_test(
        &self,
    ) -> tokio::sync::OwnedRwLockWriteGuard<InternalMetadata> {
        Arc::clone(&self.metadata).write_owned().await
    }
}

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    fn persist_metadata_snapshot(
        store: Option<&MetadataStateStore>,
        previous: &InternalMetadata,
        candidate: &InternalMetadata,
    ) -> Result<(), ServerError> {
        let Some(store) = store else {
            return Ok(());
        };
        let receipt = store.commit(previous, candidate)?;
        receipt.acknowledge();
        Ok(())
    }

    async fn commit_publication_metadata(
        &self,
        artifact_id: ArtifactId,
        expected_previous: Option<&Item>,
        item: Item,
    ) -> Result<(), ServerError> {
        let live_metadata = Arc::clone(&self.metadata);
        let persist_lock = Arc::clone(&self.metadata_persist_lock);
        let store = self.metadata_store.clone();
        let expected_previous = expected_previous.cloned();
        tokio::task::spawn_blocking(move || {
            let _persist_guard = persist_lock
                .lock()
                .map_err(|_| ServerError::State("metadata commit lock is poisoned".to_string()))?;
            let snapshot = live_metadata.blocking_read();
            let mut next = (*snapshot).clone();
            let actual_previous = current_publication_item(&next, &artifact_id)?;
            if actual_previous != expected_previous {
                return Err(ServerError::State(
                    "publication history changed before metadata commit".to_string(),
                ));
            }
            let versions = next.versions.entry(artifact_id.clone()).or_default();
            if versions.is_empty() {
                if let Some(previous) = actual_previous {
                    match versions.entry(previous.manifest.version) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(previous);
                        }
                        std::collections::btree_map::Entry::Occupied(_) => unreachable!(),
                    }
                }
            }
            match versions.entry(item.manifest.version) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(item.clone());
                }
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(ServerError::State(
                        "new publication conflicts with an existing immutable revision".to_string(),
                    ));
                }
            }
            next.items.insert(artifact_id, item);
            Self::persist_metadata_snapshot(store.as_deref(), &snapshot, &next)?;
            drop(snapshot);
            *live_metadata.blocking_write() = next;
            Ok::<_, ServerError>(())
        })
        .await
        .map_err(|error| ServerError::State(format!("metadata worker failed: {error}")))??;
        Ok(())
    }

    async fn ensure_publication_history(
        &self,
        artifact_id: &str,
        item: &Item,
    ) -> Result<(), ServerError> {
        if self
            .metadata
            .read()
            .await
            .versions
            .get(artifact_id)
            .is_some_and(|versions| versions.get(&item.manifest.version) == Some(item))
        {
            return Ok(());
        }
        let live_metadata = Arc::clone(&self.metadata);
        let persist_lock = Arc::clone(&self.metadata_persist_lock);
        let store = self.metadata_store.clone();
        let artifact_id = artifact_id.to_string();
        let item = item.clone();
        tokio::task::spawn_blocking(move || {
            let _persist_guard = persist_lock
                .lock()
                .map_err(|_| ServerError::State("metadata commit lock is poisoned".to_string()))?;
            let snapshot = live_metadata.blocking_read();
            let mut next = (*snapshot).clone();
            if next.items.get(&artifact_id) != Some(&item) {
                return Err(ServerError::State(
                    "publication changed before historical backfill".to_string(),
                ));
            }
            let versions = next.versions.entry(artifact_id).or_default();
            match versions.entry(item.manifest.version) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(item);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &item => {}
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(ServerError::State(
                        "publication history conflicts during backfill".to_string(),
                    ));
                }
            }
            Self::persist_metadata_snapshot(store.as_deref(), &snapshot, &next)?;
            drop(snapshot);
            *live_metadata.blocking_write() = next;
            Ok::<_, ServerError>(())
        })
        .await
        .map_err(|error| ServerError::State(format!("metadata worker failed: {error}")))??;
        Ok(())
    }

    /// Create a new server instance with a loaded key pair, initial metadata and storage.
    ///
    /// The `uuid_nonce` is derived from a random UUID (not from the key bytes) so that it
    /// remains unique across restarts even when the same key is reused.
    pub fn new(
        pkcs8_bytes: &Document,
        storage: T,
        metadata: InternalMetadata,
    ) -> Result<Self, KeyRejected> {
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref())?;
        // Use a cryptographically random nonce – never derived from key material.
        let uuid_nonce = Uuid::new_v4().to_string();
        Ok(Self {
            key_pair: Arc::new(key_pair),
            uuid_nonce,
            enrollment_token: None,
            metadata: Arc::new(RwLock::new(metadata)),
            storage: Arc::new(storage),
            clients: Arc::new(RwLock::new(BTreeMap::new())),
            publish_locks: Arc::new(AsyncMutex::new(HashMap::new())),
            metadata_persist_lock: Arc::new(StdMutex::new(())),
            metadata_store: None,
        })
    }

    /// Create a server with a freshly generated ephemeral key pair and a default storage.
    ///
    /// **Not recommended for production.** In production use [`Server::new`] and persist the key
    /// on disk so that the server identity is stable across restarts.
    pub fn new_ephemeral(storage: T) -> Self {
        Self::new_ephemeral_with_metadata(storage, InternalMetadata::default())
    }

    pub fn new_ephemeral_with_metadata(storage: T, metadata: InternalMetadata) -> Self {
        let rng = rand::SystemRandom::new();
        let pkcs8_bytes = signature::Ed25519KeyPair::generate_pkcs8(&rng).unwrap();
        let key_pair = signature::Ed25519KeyPair::from_pkcs8(pkcs8_bytes.as_ref()).unwrap();
        Self {
            key_pair: Arc::new(key_pair),
            uuid_nonce: Uuid::new_v4().to_string(),
            enrollment_token: None,
            metadata: Arc::new(RwLock::new(metadata)),
            storage: Arc::new(storage),
            clients: Arc::new(RwLock::new(BTreeMap::new())),
            publish_locks: Arc::new(AsyncMutex::new(HashMap::new())),
            metadata_persist_lock: Arc::new(StdMutex::new(())),
            metadata_store: None,
        }
    }

    #[must_use]
    pub fn with_enrollment_token(mut self, token: impl Into<Arc<str>>) -> Self {
        let token = token.into();
        self.enrollment_token = (!token.trim().is_empty()).then_some(token);
        self
    }

    #[must_use]
    pub fn with_metadata_persistence(mut self, path: PathBuf) -> Self {
        self.metadata_store = Some(MetadataStateStore::for_current_path(path));
        self
    }

    #[must_use]
    pub fn with_metadata_store(mut self, store: Arc<MetadataStateStore>) -> Self {
        self.metadata_store = Some(store);
        self
    }

    async fn publication_guard(&self, artifact_id: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.publish_locks.lock().await;
            Arc::clone(
                locks
                    .entry(artifact_id.to_string())
                    .or_insert_with(|| Arc::new(AsyncMutex::new(()))),
            )
        };
        lock.lock_owned().await
    }
}

#[derive(Debug)]
pub struct RegisterError;

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
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
        let requested_uuid = request.uuid.as_deref().map(slice_to_uuid);
        let known_uuid = match requested_uuid {
            Some(uuid) if self.clients.read().await.contains_key(&uuid) => Some(uuid),
            _ => None,
        };
        if known_uuid.is_none() {
            let expected = self
                .enrollment_token
                .as_deref()
                .ok_or_else(|| String::from("Client enrollment is disabled"))?;
            let provided = request
                .enrollment_token
                .as_deref()
                .ok_or_else(|| String::from("Invalid enrollment token"))?;
            ring::constant_time::verify_slices_are_equal(expected.as_bytes(), provided.as_bytes())
                .map_err(|_| String::from("Invalid enrollment token"))?;
        }
        let uuid = self
            .register_client(
                request.name,
                addr,
                Version::from_str(&request.version).ok(),
                known_uuid,
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
    ) -> Result<(proto::ManifestResponse, Item), String> {
        let item = {
            let md = self.metadata.read().await;
            match manifest_req.version {
                Some(version) => md
                    .versions
                    .get(&manifest_req.artifact_id)
                    .and_then(|versions| versions.get(&version))
                    .cloned(),
                None => md.items.get(&manifest_req.artifact_id).cloned(),
            }
        }
        .ok_or_else(|| format!("Unknown artifact: {}", manifest_req.artifact_id))?;

        let root_hash = *item.root();

        Ok((
            proto::ManifestResponse {
                artifact_id: item.manifest.artifact_id.clone(),
                version: item.manifest.version,
                root_hash: root_hash.as_bytes().to_vec(),
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
                chunk_sizes: item.chunks.iter().map(|chunk| chunk.size as u32).collect(),
            },
            item,
        ))
    }

    pub async fn sync_chunk_response_messages(
        &self,
        item: &Item,
        possession: proto::PossessionBitfield,
    ) -> Result<Vec<SyncMessage>, String> {
        let root_hash = *item.root();
        let expected_chunk_count = item.chunks.len();
        let bitfield = Bitfield::from_bytes(&possession.bitfield)
            .ok_or_else(|| String::from("Invalid bitfield"))?;
        if bitfield.chunk_count() as usize != expected_chunk_count {
            return Err(String::from("Invalid bitfield"));
        }

        let mut messages = Vec::new();
        let likely_precompressed =
            chunk_algorithm_likely_precompressed(item.manifest.chunk_algorithm);

        let missing = bitfield.missing_indices();
        tracing::debug!(
            "Sync: {} missing chunks out of {}",
            missing.len(),
            bitfield.chunk_count()
        );

        for batch in build_sync_payload_batches(
            &missing,
            Some(item.metadata.path.as_path()),
            likely_precompressed,
            |idx| self.storage.get_chunk_by_index(&root_hash, idx),
        )? {
            messages.push(batch);
        }

        Ok(messages)
    }

    /// Prepare the sync state (bitfield validation + missing indices) without
    /// building all response messages up front. Returns the data needed to
    /// stream batches one at a time via [`build_one_batch`].
    pub fn prepare_sync_chunks(
        &self,
        item: &Item,
        possession: &proto::PossessionBitfield,
    ) -> Result<SyncChunkIterState, String> {
        let root_hash = *item.root();
        let expected_chunk_count = item.chunks.len();
        let bitfield = Bitfield::from_bytes(&possession.bitfield)
            .ok_or_else(|| String::from("Invalid bitfield"))?;
        if bitfield.chunk_count() as usize != expected_chunk_count {
            return Err(String::from("Invalid bitfield"));
        }

        let likely_precompressed =
            chunk_algorithm_likely_precompressed(item.manifest.chunk_algorithm);
        let missing = bitfield.missing_indices();
        tracing::info!(
            distd_sync_manifest_chunks = expected_chunk_count,
            distd_sync_missing_chunks = missing.len(),
            "distd_sync_selection"
        );

        Ok(SyncChunkIterState {
            root_hash,
            missing,
            likely_precompressed,
            cursor: 0,
        })
    }

    pub async fn sync_response_messages(
        &self,
        manifest_req: proto::ManifestRequest,
        possession: proto::PossessionBitfield,
    ) -> Result<Vec<SyncMessage>, String> {
        let (manifest, item) = self.sync_manifest_response(manifest_req).await?;
        let mut messages = Vec::with_capacity(1);
        messages.push(SyncMessage {
            msg: Some(Msg::ManifestResponse(manifest)),
        });
        messages.extend(self.sync_chunk_response_messages(&item, possession).await?);
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

        tracing::info!(
            "Got new client: \"{}\" ver:{:?}, @{}, {:?}",
            name,
            version,
            addr,
            uuid
        );
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
        let coordinator = self.clone();
        tokio::spawn(async move { coordinator.expose_feed_coordinated(feed).await })
            .await
            .map_err(|_| RegisterError)?
    }

    async fn expose_feed_coordinated(&self, feed: Feed) -> Result<FeedName, RegisterError> {
        self.require_publication_writable()
            .map_err(|_| RegisterError)?;
        let live_metadata = Arc::clone(&self.metadata);
        let persist_lock = Arc::clone(&self.metadata_persist_lock);
        let store = self.metadata_store.clone();
        let storage = Arc::clone(&self.storage);
        let name = feed.name.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _persist_guard = persist_lock
                .lock()
                .map_err(|_| ServerError::State("metadata commit lock is poisoned".to_string()))?;
            let snapshot = live_metadata.blocking_read();
            if store.as_ref().is_some_and(|store| store.is_poisoned()) {
                return Err(ServerError::State(
                    "metadata publication is read-only until restart".to_string(),
                ));
            }
            for item in feed.paths.values() {
                storage
                    .verify_committed_revision(item)
                    .map_err(|_| ServerError::ChunkInsertError)?;
            }
            let mut next = (*snapshot).clone();
            match next.feeds.entry(name.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(feed);
                }
                std::collections::hash_map::Entry::Occupied(_) => {
                    return Err(ServerError::State("feed already exists".to_string()));
                }
            }
            crate::metadata_state::validate_runtime_metadata(&next)?;
            Self::persist_metadata_snapshot(store.as_deref(), &snapshot, &next)?;
            drop(snapshot);
            *live_metadata.blocking_write() = next;
            Ok::<_, ServerError>(name)
        })
        .await
        .map_err(|_| RegisterError)?
        .map_err(|_| RegisterError)?;
        Ok(result)
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
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, ServerError> {
        let artifact_id = name.clone();
        self.publish_item_as(artifact_id, name, path, description, file, chunk_algorithm)
            .await
    }

    pub async fn publish_item_as(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        file: Bytes,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, ServerError>
    where
        T: 'static,
    {
        let coordinator = self.clone();
        tokio::spawn(async move {
            coordinator
                .publish_item_as_coordinated(
                    artifact_id,
                    name,
                    path,
                    description,
                    file,
                    chunk_algorithm,
                )
                .await
        })
        .await
        .map_err(|error| ServerError::State(format!("publication coordinator failed: {error}")))?
    }

    async fn publish_item_as_coordinated(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        file: Bytes,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, ServerError> {
        let storage = Arc::clone(&self.storage);
        let prepare_artifact_id = artifact_id.clone();
        let prepare_file = file.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            storage.prepare_revision(
                prepare_artifact_id,
                name,
                path,
                0,
                description,
                prepare_file,
                chunk_algorithm,
            )
        })
        .await
        .map_err(|error| ServerError::State(format!("revision prepare worker failed: {error}")))?
        .map_err(|error| {
            tracing::error!("Cannot prepare revision: {error}");
            ServerError::ChunkInsertError
        })?;
        let _publish_guard = self.publication_guard(&artifact_id).await;
        self.require_publication_writable()?;
        let previous = {
            let metadata = self.metadata.read().await;
            current_publication_item(&metadata, &artifact_id)?
        };
        if let Some(old) = previous.as_ref() {
            if same_version_relevant_metadata(old, &prepared) {
                self.ensure_publication_history(&artifact_id, old).await?;
                return Ok(old.clone());
            }
        }
        let revision = previous.as_ref().map_or(Ok(0), |item| {
            item.metadata
                .revision
                .checked_add(1)
                .ok_or_else(|| ServerError::State("artifact revision is exhausted".to_string()))
        })?;
        let storage = Arc::clone(&self.storage);
        let item = tokio::task::spawn_blocking(move || {
            storage.commit_revision(prepared.with_revision(revision), file)
        })
        .await
        .map_err(|error| ServerError::State(format!("revision storage worker failed: {error}")))?
        .map_err(|error| {
            tracing::error!("Storage error: {error}");
            ServerError::ChunkInsertError
        })?;

        if let Err(error) = self
            .commit_publication_metadata(artifact_id, previous.as_ref(), item.clone())
            .await
        {
            self.cleanup_failed_publication(&item).await?;
            return Err(error);
        }

        Ok(item)
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn publish_item_from_files(
        &self,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        files: Vec<(PathBuf, Bytes)>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, ServerError> {
        let artifact_id = name.clone();
        self.publish_item_from_files_as(
            artifact_id,
            name,
            path,
            description,
            files,
            chunk_algorithm,
        )
        .await
    }

    pub async fn publish_item_from_files_as(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        files: Vec<(PathBuf, Bytes)>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, ServerError>
    where
        T: 'static,
    {
        let coordinator = self.clone();
        tokio::spawn(async move {
            coordinator
                .publish_item_from_files_as_coordinated(
                    artifact_id,
                    name,
                    path,
                    description,
                    files,
                    chunk_algorithm,
                )
                .await
        })
        .await
        .map_err(|error| ServerError::State(format!("publication coordinator failed: {error}")))?
    }

    async fn publish_item_from_files_as_coordinated(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        mut files: Vec<(PathBuf, Bytes)>,
        chunk_algorithm: ChunkAlgorithm,
    ) -> Result<Item, ServerError> {
        files.sort_by(|left, right| left.0.cmp(&right.0));
        let storage = Arc::clone(&self.storage);
        let prepare_artifact_id = artifact_id.clone();
        let prepare_files = files.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            storage.prepare_revision_from_files(
                prepare_artifact_id,
                name,
                path,
                0,
                description,
                prepare_files,
                chunk_algorithm,
            )
        })
        .await
        .map_err(|error| {
            ServerError::State(format!("directory revision prepare worker failed: {error}"))
        })?
        .map_err(|error| {
            tracing::error!("Cannot prepare directory revision: {error}");
            ServerError::ChunkInsertError
        })?;

        let _publish_guard = self.publication_guard(&artifact_id).await;
        self.require_publication_writable()?;
        let previous = {
            let metadata = self.metadata.read().await;
            current_publication_item(&metadata, &artifact_id)?
        };
        if let Some(old) = previous.as_ref() {
            if same_version_relevant_metadata(old, &prepared) {
                self.ensure_publication_history(&artifact_id, old).await?;
                return Ok(old.clone());
            }
        }
        let revision = previous.as_ref().map_or(Ok(0), |item| {
            item.metadata
                .revision
                .checked_add(1)
                .ok_or_else(|| ServerError::State("artifact revision is exhausted".to_string()))
        })?;
        let storage = Arc::clone(&self.storage);
        let item = tokio::task::spawn_blocking(move || {
            storage.commit_revision_from_files(prepared.with_revision(revision), files)
        })
        .await
        .map_err(|error| ServerError::State(format!("revision storage worker failed: {error}")))?
        .map_err(|error| {
            tracing::error!("Storage error: {error}");
            ServerError::ChunkInsertError
        })?;

        if let Err(error) = self
            .commit_publication_metadata(artifact_id, previous.as_ref(), item.clone())
            .await
        {
            self.cleanup_failed_publication(&item).await?;
            return Err(error);
        }

        Ok(item)
    }

    async fn cleanup_failed_publication(&self, item: &Item) -> Result<(), ServerError> {
        if self
            .metadata_store
            .as_ref()
            .is_some_and(|store| store.is_poisoned())
        {
            return Ok(());
        }
        let storage = Arc::clone(&self.storage);
        let item = item.clone();
        tokio::task::spawn_blocking(move || storage.discard_unpublished_revision(&item))
            .await
            .map_err(|error| {
                ServerError::State(format!("revision cleanup worker failed: {error}"))
            })?
            .map_err(|error| {
                ServerError::State(format!("cannot discard unpublished revision: {error}"))
            })
    }

    fn require_publication_writable(&self) -> Result<(), ServerError> {
        if self
            .metadata_store
            .as_ref()
            .is_some_and(|store| store.is_poisoned())
        {
            return Err(ServerError::State(
                "metadata publication is read-only until restart".to_string(),
            ));
        }
        Ok(())
    }

    /// Get the public key of the server
    #[must_use]
    pub fn public_key(&self) -> &[u8] {
        self.key_pair.public_key().as_ref()
    }
}

fn current_publication_item(
    metadata: &InternalMetadata,
    artifact_id: &str,
) -> Result<Option<Item>, ServerError> {
    let current = metadata.items.get(artifact_id).cloned();
    if let Some(current) = &current {
        validate_item_identity(artifact_id, current)?;
    }
    let historical = if let Some(versions) = metadata.versions.get(artifact_id) {
        for (version, item) in versions {
            validate_item_identity(artifact_id, item)?;
            if *version != item.manifest.version {
                return Err(ServerError::State(format!(
                    "artifact {artifact_id} history key disagrees with its item version"
                )));
            }
        }
        versions
            .last_key_value()
            .map(|(version, item)| (*version, item.clone()))
    } else {
        None
    };
    match (&current, &historical) {
        (None, None) => Ok(None),
        (Some(current), Some((version, historical)))
            if current == historical && *version == current.manifest.version =>
        {
            Ok(current.clone().into())
        }
        (Some(current), None) => Ok(current.clone().into()),
        (None, Some(_)) | (Some(_), Some(_)) => Err(ServerError::State(format!(
            "artifact {artifact_id} current revision disagrees with immutable history"
        ))),
    }
}

const BULK_TARGET_BYTES: usize = 16 * 1024 * 1024;
const BULK_MAX_CHUNKS: usize = 512;

/// State for streaming sync chunk batches one at a time.
pub struct SyncChunkIterState {
    pub root_hash: Hash,
    pub missing: Vec<u32>,
    pub likely_precompressed: bool,
    pub cursor: usize,
}

pub fn build_one_batch<F>(
    missing: &[u32],
    cursor: &mut usize,
    primary_path: Option<&Path>,
    likely_precompressed: bool,
    load_chunk: &mut F,
) -> Result<Option<SyncMessage>, String>
where
    F: FnMut(u32) -> Option<Vec<u8>>,
{
    while *cursor < missing.len() {
        let start_index = missing[*cursor];
        let mut current_index = start_index;
        let mut count = 0usize;
        let mut bulk_buf = Vec::with_capacity(BULK_TARGET_BYTES);

        while *cursor < missing.len() && count < BULK_MAX_CHUNKS {
            let idx = missing[*cursor];
            if idx != current_index {
                break;
            }

            let Some(data) = load_chunk(idx) else {
                if count == 0 {
                    *cursor += 1;
                }
                break;
            };

            if count > 0 && bulk_buf.len() + data.len() > BULK_TARGET_BYTES {
                break;
            }

            bulk_buf.extend_from_slice(&data);
            count += 1;
            *cursor += 1;
            current_index += 1;
        }

        if count == 0 {
            continue;
        }

        let encoded =
            maybe_compress_sync_payload_owned(bulk_buf, primary_path, likely_precompressed)
                .map_err(|err| err.to_string())?;

        let message = if count == 1 {
            SyncMessage {
                msg: Some(Msg::ChunkData(proto::ChunkData {
                    chunk_index: start_index,
                    data: encoded.data,
                    compression: encoded.compression,
                    uncompressed_size: encoded.uncompressed_size,
                })),
            }
        } else {
            SyncMessage {
                msg: Some(Msg::BulkData(proto::BulkData {
                    data: encoded.data,
                    start_index,
                    count: count as u32,
                    compression: encoded.compression,
                    uncompressed_size: encoded.uncompressed_size,
                })),
            }
        };
        return Ok(Some(message));
    }
    Ok(None)
}

fn build_sync_payload_batches<F>(
    missing: &[u32],
    primary_path: Option<&Path>,
    likely_precompressed: bool,
    mut load_chunk: F,
) -> Result<Vec<SyncMessage>, String>
where
    F: FnMut(u32) -> Option<Vec<u8>>,
{
    let mut messages = Vec::new();
    let mut cursor = 0;
    while let Some(msg) = build_one_batch(
        missing,
        &mut cursor,
        primary_path,
        likely_precompressed,
        &mut load_chunk,
    )? {
        messages.push(msg);
    }
    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::{build_sync_payload_batches, InternalMetadata, Server, ServerError};
    use axum::body::Bytes;
    use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
    use distd_core::chunk_storage::ChunkStorage;
    use distd_core::chunks::ChunkAlgorithm;
    use distd_core::durable_state::{
        replace_enveloped_state, DurableReplaceStage, DurableStateError, DurableStateKind,
    };
    use distd_core::proto::ManifestRequest;
    use distd_core::proto::{sync_message::Msg, PayloadCompression};
    use std::path::Path;
    use std::sync::{Arc, Barrier};

    #[derive(Debug)]
    struct BlockingMetadataReplace {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl crate::metadata_state::MetadataReplace for BlockingMetadataReplace {
        fn replace(&self, path: &Path, payload: &[u8]) -> Result<(), DurableStateError> {
            self.entered.wait();
            self.release.wait();
            replace_enveloped_state(
                path,
                DurableStateKind::ServerPublication,
                crate::metadata_state::SERVER_PUBLICATION_SCHEMA,
                payload,
                crate::metadata_state::MAX_SERVER_PUBLICATION_PAYLOAD,
            )
        }
    }

    #[test]
    fn builds_bulk_messages_for_contiguous_partial_runs() {
        let messages = build_sync_payload_batches(&[1, 2, 4], None, false, |idx| match idx {
            1 => Some(vec![1, 2]),
            2 => Some(vec![3, 4]),
            4 => Some(vec![9]),
            _ => None,
        })
        .expect("build sync payload batches");

        assert_eq!(messages.len(), 2);

        match &messages[0].msg {
            Some(Msg::BulkData(bulk)) => {
                assert_eq!(bulk.start_index, 1);
                assert_eq!(bulk.count, 2);
                assert_eq!(bulk.data, vec![1, 2, 3, 4]);
                assert_eq!(bulk.compression, PayloadCompression::Unspecified as i32);
            }
            other => panic!("expected bulk message, got {other:?}"),
        }

        match &messages[1].msg {
            Some(Msg::ChunkData(chunk)) => {
                assert_eq!(chunk.chunk_index, 4);
                assert_eq!(chunk.data, vec![9]);
                assert_eq!(chunk.compression, PayloadCompression::Unspecified as i32);
            }
            other => panic!("expected chunk message, got {other:?}"),
        }
    }

    #[test]
    fn skips_missing_chunks_absent_from_storage() {
        let messages = build_sync_payload_batches(&[0, 1, 2], None, false, |idx| match idx {
            1 => Some(vec![7, 8]),
            2 => Some(vec![9, 10]),
            _ => None,
        })
        .expect("build sync payload batches");

        assert_eq!(messages.len(), 1);
        match &messages[0].msg {
            Some(Msg::BulkData(bulk)) => {
                assert_eq!(bulk.start_index, 1);
                assert_eq!(bulk.count, 2);
            }
            other => panic!("expected bulk message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn adaptive_publication_is_idempotent_and_versions_are_retrievable() {
        let server = Server::new_ephemeral(HashMapStorage::default());
        let algorithm = ChunkAlgorithm::fastcdc_default();
        let original = Bytes::from(vec![7u8; 2 * 1024 * 1024]);

        let first = server
            .publish_item_as(
                "stable-id".to_string(),
                "display-name".to_string(),
                "artifact.bin".into(),
                None,
                original.clone(),
                algorithm,
            )
            .await
            .unwrap();
        let duplicate = server
            .publish_item_as(
                "stable-id".to_string(),
                "display-name".to_string(),
                "artifact.bin".into(),
                None,
                original,
                algorithm,
            )
            .await
            .unwrap();
        assert_eq!(duplicate.metadata.revision, first.metadata.revision);

        let second = server
            .publish_item_as(
                "stable-id".to_string(),
                "renamed-display".to_string(),
                "renamed.bin".into(),
                None,
                Bytes::from(vec![8u8; 2 * 1024 * 1024]),
                algorithm,
            )
            .await
            .unwrap();
        assert_eq!(second.metadata.artifact_id, "stable-id");
        assert_eq!(second.metadata.revision, first.metadata.revision + 1);

        let (_, historical) = server
            .sync_manifest_response(ManifestRequest {
                artifact_id: "stable-id".to_string(),
                version: Some(first.manifest.version),
            })
            .await
            .unwrap();
        assert_eq!(historical.metadata.root, first.metadata.root);
    }

    #[tokio::test]
    async fn filesystem_exact_versions_survive_same_path_publication_and_restart() {
        use distd_core::chunk_storage::fs_storage::FsStorage;

        let root = std::env::temp_dir().join(format!(
            "distd-server-revision-test-{}",
            uuid::Uuid::new_v4()
        ));
        let storage = FsStorage::new(root.clone()).unwrap();
        let server = Server::new_ephemeral(storage);
        let algorithm = ChunkAlgorithm::default();
        let first_bytes = Bytes::from(vec![1u8; distd_core::chunks::CHUNK_SIZE * 2]);
        let mut second_vec = first_bytes.to_vec();
        second_vec[distd_core::chunks::CHUNK_SIZE] = 2;
        let second_bytes = Bytes::from(second_vec);

        let first = server
            .publish_item_as(
                "stable-id".to_string(),
                "display".to_string(),
                "same.bin".into(),
                None,
                first_bytes.clone(),
                algorithm,
            )
            .await
            .unwrap();
        let second = server
            .publish_item_as(
                "stable-id".to_string(),
                "display".to_string(),
                "same.bin".into(),
                None,
                second_bytes.clone(),
                algorithm,
            )
            .await
            .unwrap();
        let metadata = server.metadata.read().await.clone();
        drop(server);

        let restarted =
            Server::new_ephemeral_with_metadata(FsStorage::new(root).unwrap(), metadata);
        for (item, expected) in [(&first, &first_bytes), (&second, &second_bytes)] {
            let (_, exact) = restarted
                .sync_manifest_response(ManifestRequest {
                    artifact_id: "stable-id".to_string(),
                    version: Some(item.manifest.version),
                })
                .await
                .unwrap();
            let actual = exact
                .chunks
                .iter()
                .enumerate()
                .flat_map(|(index, _)| {
                    restarted
                        .storage
                        .get_chunk_by_index(&exact.metadata.root.hash, index as u32)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, expected.as_ref());
        }
    }

    #[tokio::test]
    async fn metadata_change_versions_identical_bytes_and_revision_exhaustion_fails() {
        let server = Server::new_ephemeral(HashMapStorage::default());
        let bytes = Bytes::from_static(b"same payload");
        let algorithm = ChunkAlgorithm::default();
        let first = server
            .publish_item_as(
                "stable-id".to_string(),
                "first".to_string(),
                "same.bin".into(),
                None,
                bytes.clone(),
                algorithm,
            )
            .await
            .unwrap();
        let second = server
            .publish_item_as(
                "stable-id".to_string(),
                "renamed".to_string(),
                "same.bin".into(),
                None,
                bytes.clone(),
                algorithm,
            )
            .await
            .unwrap();
        assert_eq!(second.metadata.revision, first.metadata.revision + 1);

        let mut exhausted = second.clone().with_revision(u32::MAX);
        exhausted.metadata.artifact_id = "exhausted".to_string();
        exhausted.manifest.artifact_id = "exhausted".to_string();
        server
            .metadata
            .write()
            .await
            .items
            .insert("exhausted".to_string(), exhausted);
        let error = server
            .publish_item_as(
                "exhausted".to_string(),
                "different".to_string(),
                "same.bin".into(),
                None,
                bytes,
                algorithm,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, ServerError::State(message) if message.contains("exhausted")));
    }

    #[tokio::test]
    async fn metadata_load_rejects_current_behind_history_and_mismatched_version_keys() {
        let server = Server::new_ephemeral(HashMapStorage::default());
        let algorithm = ChunkAlgorithm::default();
        let first = server
            .publish_item_as(
                "stable-id".to_string(),
                "display".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"revision zero"),
                algorithm,
            )
            .await
            .unwrap();
        let second = server
            .publish_item_as(
                "stable-id".to_string(),
                "display".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"revision one"),
                algorithm,
            )
            .await
            .unwrap();

        let mut behind = server.metadata.read().await.clone();
        behind.items.insert("stable-id".to_string(), first);
        let behind_error = crate::metadata_state::validate_runtime_metadata(&behind).unwrap_err();
        assert!(matches!(
            behind_error,
            ServerError::State(message) if message.contains("highest historical revision")
        ));

        let mut mismatched_key = server.metadata.read().await.clone();
        let versions = mismatched_key.versions.get_mut("stable-id").unwrap();
        versions.remove(&second.manifest.version);
        versions.insert(second.manifest.version + 1, second);
        let mismatched_error =
            crate::metadata_state::validate_runtime_metadata(&mismatched_key).unwrap_err();
        assert!(matches!(
            mismatched_error,
            ServerError::State(message) if message.contains("history key disagrees")
        ));
    }

    #[tokio::test]
    async fn publication_backfills_current_only_history_and_rejects_malformed_live_history() {
        let algorithm = ChunkAlgorithm::default();
        let seed = Server::new_ephemeral(HashMapStorage::default());
        let first = seed
            .publish_item_as(
                "stable-id".to_string(),
                "display".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"revision zero"),
                algorithm,
            )
            .await
            .unwrap();

        let mut current_only = InternalMetadata::default();
        current_only
            .items
            .insert("stable-id".to_string(), first.clone());
        let server = Server::new_ephemeral_with_metadata(HashMapStorage::default(), current_only);
        let duplicate = server
            .publish_item_as(
                "stable-id".to_string(),
                "display".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"revision zero"),
                algorithm,
            )
            .await
            .unwrap();
        assert_eq!(duplicate, first);
        let (_, exact_first) = server
            .sync_manifest_response(ManifestRequest {
                artifact_id: "stable-id".to_string(),
                version: Some(first.manifest.version),
            })
            .await
            .unwrap();
        assert_eq!(exact_first, first);
        let second = server
            .publish_item_as(
                "stable-id".to_string(),
                "display".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"revision one"),
                algorithm,
            )
            .await
            .unwrap();
        let metadata = server.metadata.read().await;
        let versions = metadata.versions.get("stable-id").unwrap();
        assert_eq!(versions.get(&first.manifest.version), Some(&first));
        assert_eq!(versions.get(&second.manifest.version), Some(&second));
        drop(metadata);

        let mut malformed = server.metadata.read().await.clone();
        let versions = malformed.versions.get_mut("stable-id").unwrap();
        let historical = versions.remove(&second.manifest.version).unwrap();
        versions.insert(second.manifest.version + 1, historical);
        let storage = HashMapStorage::default();
        let malformed_server = Server::new_ephemeral_with_metadata(storage, malformed.clone());
        let error = malformed_server
            .publish_item_as(
                "stable-id".to_string(),
                "display".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"revision two"),
                algorithm,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ServerError::State(message) if message.contains("history key disagrees")
        ));
        assert!(malformed_server.storage.chunks().is_empty());
        assert_eq!(
            malformed_server
                .metadata
                .read()
                .await
                .versions
                .get("stable-id"),
            malformed.versions.get("stable-id")
        );
    }

    #[tokio::test]
    async fn failed_metadata_persistence_never_becomes_idempotent_success() {
        let parent_file = std::env::temp_dir().join(format!(
            "distd-server-metadata-parent-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&parent_file, b"not a directory").unwrap();
        let metadata_path = parent_file.join("metadata.state");
        let server = Server::new_ephemeral(HashMapStorage::default())
            .with_metadata_persistence(metadata_path.clone());
        let algorithm = ChunkAlgorithm::default();

        for _ in 0..2 {
            let result = server
                .publish_item_as(
                    "stable-id".to_string(),
                    "display".to_string(),
                    "artifact.bin".into(),
                    None,
                    Bytes::from_static(b"durable candidate"),
                    algorithm,
                )
                .await;
            assert!(result.is_err());
            assert!(server.metadata.read().await.items.is_empty());
            assert!(server.metadata.read().await.versions.is_empty());
        }

        assert!(std::fs::symlink_metadata(metadata_path).is_err());
    }

    #[tokio::test]
    async fn dropping_publication_waiter_does_not_cancel_the_owned_coordinator() {
        let directory = std::env::temp_dir().join(format!(
            "distd-server-cancelled-publication-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let server = Server::new_ephemeral_with_metadata(HashMapStorage::default(), metadata)
            .with_metadata_store(store);
        let waiting_server = server.clone();
        let waiter = tokio::spawn(async move {
            waiting_server
                .publish_item_as(
                    "stable-id".to_string(),
                    "display".to_string(),
                    "artifact.bin".into(),
                    None,
                    Bytes::from(vec![7_u8; 8 * 1024 * 1024]),
                    ChunkAlgorithm::default(),
                )
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        waiter.abort();

        let mut published = None;
        for _ in 0..200 {
            if let Some(item) = server.metadata.read().await.items.get("stable-id").cloned() {
                published = Some(item);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let published = published.expect("owned coordinator completes after waiter cancellation");
        let (_, exact) = server
            .sync_manifest_response(ManifestRequest {
                artifact_id: "stable-id".to_string(),
                version: Some(published.manifest.version),
            })
            .await
            .unwrap();
        assert_eq!(exact, published);
        let (reopened, durable) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |_| Ok(()))
                .unwrap();
        drop(reopened);
        assert_eq!(durable.items.get("stable-id"), Some(&published));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn every_metadata_replace_stage_preserves_live_visibility_and_recovers_exactly() {
        let stages = [
            DurableReplaceStage::AfterEncode,
            DurableReplaceStage::AfterCreate,
            DurableReplaceStage::AfterWrite,
            DurableReplaceStage::AfterFileSync,
            DurableReplaceStage::AfterRename,
            DurableReplaceStage::AfterDirectorySync,
        ];
        for (stage_index, stage) in stages.into_iter().enumerate() {
            let directory = std::env::temp_dir().join(format!(
                "distd-server-publication-stage-{stage:?}-{}",
                uuid::Uuid::new_v4()
            ));
            std::fs::create_dir(&directory).unwrap();
            let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
            let (initial_store, _) =
                crate::metadata_state::MetadataStateStore::open_with_validator(
                    paths.clone(),
                    |_| Ok(()),
                )
                .unwrap();
            drop(initial_store);
            let (store, metadata) = crate::metadata_state::MetadataStateStore::open_with_replacer(
                paths.clone(),
                crate::metadata_state::StageFailingMetadataReplace::new(stage),
                |_| Ok(()),
            )
            .unwrap();
            let server =
                Server::new_ephemeral_with_metadata(HashMapStorage::default(), metadata.clone())
                    .with_metadata_store(store);
            let expected_bytes: &[u8] = if stage_index.is_multiple_of(2) {
                b"complete candidate"
            } else {
                b"directory candidate"
            };
            let result = if stage_index.is_multiple_of(2) {
                server
                    .publish_item_as(
                        "faulted".to_string(),
                        "display".to_string(),
                        "artifact.bin".into(),
                        None,
                        Bytes::copy_from_slice(expected_bytes),
                        ChunkAlgorithm::default(),
                    )
                    .await
            } else {
                server
                    .publish_item_from_files_as(
                        "faulted".to_string(),
                        "display".to_string(),
                        "artifact".into(),
                        None,
                        vec![(
                            "nested/file.bin".into(),
                            Bytes::copy_from_slice(expected_bytes),
                        )],
                        ChunkAlgorithm::default(),
                    )
                    .await
            };
            assert!(result.is_err(), "stage {stage:?} unexpectedly succeeded");
            assert!(
                server.metadata_snapshot().await.items.is_empty(),
                "stage {stage:?} exposed an undurable candidate"
            );

            let (reopened, durable) =
                crate::metadata_state::MetadataStateStore::open_with_validator(
                    paths,
                    |candidate| candidate.verify_storage(server.storage()),
                )
                .unwrap();
            drop(reopened);
            match stage {
                DurableReplaceStage::AfterEncode
                | DurableReplaceStage::AfterCreate
                | DurableReplaceStage::AfterWrite
                | DurableReplaceStage::AfterFileSync => assert_eq!(durable, metadata),
                DurableReplaceStage::AfterRename | DurableReplaceStage::AfterDirectorySync => {
                    let item = durable
                        .items
                        .get("faulted")
                        .expect("complete new restart state");
                    assert_eq!(
                        server
                            .storage()
                            .get_chunk_by_index(&item.metadata.root.hash, 0),
                        Some(expected_bytes.to_vec())
                    );
                }
            }
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn filesystem_directory_publication_restarts_with_exact_metadata_and_bytes() {
        use distd_core::chunk_storage::fs_storage::FsStorage;

        let base = std::env::temp_dir().join(format!(
            "distd-server-fs-publication-restart-{}",
            uuid::Uuid::new_v4()
        ));
        let storage_root = base.join("storage");
        let state_root = base.join("state");
        std::fs::create_dir_all(&state_root).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&state_root);
        let storage = FsStorage::new(storage_root.clone()).unwrap();
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let server =
            Server::new_ephemeral_with_metadata(storage, metadata).with_metadata_store(store);
        let published = server
            .publish_item_from_files_as(
                "directory".to_string(),
                "display".to_string(),
                "logical".into(),
                None,
                vec![
                    ("a.txt".into(), Bytes::from_static(b"alpha")),
                    ("nested/b.txt".into(), Bytes::from_static(b"beta")),
                ],
                ChunkAlgorithm::default(),
            )
            .await
            .unwrap();
        drop(server);

        let reopened_storage = FsStorage::new(storage_root).unwrap();
        let (reopened_store, durable) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |metadata| {
                metadata.verify_storage(&reopened_storage)
            })
            .unwrap();
        reopened_storage
            .reconcile_published_revisions(&durable.published_revisions())
            .unwrap();
        drop(reopened_store);
        assert_eq!(durable.items.get("directory"), Some(&published));
        assert_eq!(
            reopened_storage.get_chunk_by_index(&published.metadata.root.hash, 0),
            Some(b"alpha".to_vec())
        );
        std::fs::remove_dir_all(base).unwrap();
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn redb_publication_restarts_with_exact_metadata_and_bytes() {
        use distd_core::chunk_storage::redb::RedbStorage;

        let base = std::env::temp_dir().join(format!(
            "distd-server-redb-publication-restart-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&base).unwrap();
        let database = base.join("chunks.redb");
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&base);
        let storage = RedbStorage::new(&database).unwrap();
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let server =
            Server::new_ephemeral_with_metadata(storage, metadata).with_metadata_store(store);
        let published = server
            .publish_item_as(
                "single".to_string(),
                "display".to_string(),
                "logical.bin".into(),
                None,
                Bytes::from_static(b"redb payload"),
                ChunkAlgorithm::default(),
            )
            .await
            .unwrap();
        drop(server);

        let reopened_storage = RedbStorage::new(&database).unwrap();
        let (reopened_store, durable) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |metadata| {
                metadata.verify_storage(&reopened_storage)
            })
            .unwrap();
        drop(reopened_store);
        assert_eq!(durable.items.get("single"), Some(&published));
        assert_eq!(
            reopened_storage.get_chunk_by_index(&published.metadata.root.hash, 0),
            Some(b"redb payload".to_vec())
        );
        std::fs::remove_dir_all(base).unwrap();
    }

    #[tokio::test]
    async fn concurrent_different_artifacts_merge_into_one_durable_snapshot() {
        let directory = std::env::temp_dir().join(format!(
            "distd-server-concurrent-publication-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let server = Server::new_ephemeral_with_metadata(HashMapStorage::default(), metadata)
            .with_metadata_store(store);
        let left_server = server.clone();
        let right_server = server.clone();
        let (left, right) = tokio::join!(
            left_server.publish_item_as(
                "left".to_string(),
                "left".to_string(),
                "left.bin".into(),
                None,
                Bytes::from_static(b"left"),
                ChunkAlgorithm::default(),
            ),
            right_server.publish_item_as(
                "right".to_string(),
                "right".to_string(),
                "right.bin".into(),
                None,
                Bytes::from_static(b"right"),
                ChunkAlgorithm::default(),
            )
        );
        let left = left.unwrap();
        let right = right.unwrap();
        let (_, durable) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |metadata| {
                metadata.verify_storage(server.storage())
            })
            .unwrap();
        assert_eq!(durable.items.get("left"), Some(&left));
        assert_eq!(durable.items.get("right"), Some(&right));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn readers_see_only_the_old_revision_while_durable_commit_is_stalled() {
        let directory = std::env::temp_dir().join(format!(
            "distd-server-stalled-publication-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let storage = HashMapStorage::default();
        let (initial_store, initial_metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let initial_server = Server::new_ephemeral_with_metadata(storage.clone(), initial_metadata)
            .with_metadata_store(initial_store);
        let old = initial_server
            .publish_item_as(
                "stable".to_string(),
                "stable".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"old revision"),
                ChunkAlgorithm::default(),
            )
            .await
            .unwrap();
        drop(initial_server);

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let replacer = Arc::new(BlockingMetadataReplace {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let (store, metadata) = crate::metadata_state::MetadataStateStore::open_with_replacer(
            paths,
            replacer,
            |candidate| candidate.verify_storage(&storage),
        )
        .unwrap();
        let server =
            Server::new_ephemeral_with_metadata(storage, metadata).with_metadata_store(store);
        let writer_server = server.clone();
        let writer = tokio::spawn(async move {
            writer_server
                .publish_item_as(
                    "stable".to_string(),
                    "stable".to_string(),
                    "artifact.bin".into(),
                    None,
                    Bytes::from_static(b"new revision"),
                    ChunkAlgorithm::default(),
                )
                .await
        });
        tokio::task::spawn_blocking(move || entered.wait())
            .await
            .unwrap();

        for _ in 0..64 {
            let (_, visible) = server
                .sync_manifest_response(ManifestRequest {
                    artifact_id: "stable".to_string(),
                    version: None,
                })
                .await
                .unwrap();
            assert_eq!(visible, old);
            tokio::task::yield_now().await;
        }

        tokio::task::spawn_blocking(move || release.wait())
            .await
            .unwrap();
        let new = writer.await.unwrap().unwrap();
        let (_, visible) = server
            .sync_manifest_response(ManifestRequest {
                artifact_id: "stable".to_string(),
                version: None,
            })
            .await
            .unwrap();
        assert_eq!(visible, new);
        assert_eq!(new.metadata.revision, old.metadata.revision + 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn concurrent_same_artifact_publications_are_monotonic_and_durable() {
        let directory = std::env::temp_dir().join(format!(
            "distd-server-concurrent-same-artifact-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let server = Server::new_ephemeral_with_metadata(HashMapStorage::default(), metadata)
            .with_metadata_store(store);
        let left_server = server.clone();
        let right_server = server.clone();
        let (left, right) = tokio::join!(
            left_server.publish_item_as(
                "stable".to_string(),
                "stable".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"left revision"),
                ChunkAlgorithm::default(),
            ),
            right_server.publish_item_as(
                "stable".to_string(),
                "stable".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"right revision"),
                ChunkAlgorithm::default(),
            )
        );
        let left = left.unwrap();
        let right = right.unwrap();
        let mut versions = [left.metadata.revision, right.metadata.revision];
        versions.sort_unstable();
        assert_eq!(versions, [0, 1]);

        let (_, durable) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |candidate| {
                candidate.verify_storage(server.storage())
            })
            .unwrap();
        let history = durable.versions.get("stable").unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history.get(&0).unwrap().metadata.revision, 0);
        assert_eq!(history.get(&1).unwrap().metadata.revision, 1);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn malformed_feed_path_is_rejected_without_live_or_durable_visibility() {
        let directory = std::env::temp_dir().join(format!(
            "distd-server-malformed-feed-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let server = Server::new_ephemeral_with_metadata(HashMapStorage::default(), metadata)
            .with_metadata_store(store);
        let payload = Bytes::from_static(b"feed payload");
        let prepared = server
            .storage()
            .prepare_revision(
                "feed-artifact".to_string(),
                "feed-artifact".to_string(),
                "right.bin".into(),
                0,
                None,
                payload.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        let item = server.storage().commit_revision(prepared, payload).unwrap();
        let mut feed = distd_core::feed::Feed::new("malformed");
        feed.paths.insert("wrong.bin".into(), item);

        assert!(server.expose_feed(feed).await.is_err());
        assert!(server.metadata_snapshot().await.feeds.is_empty());
        let (_, durable) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |_| Ok(()))
                .unwrap();
        assert!(durable.feeds.is_empty());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
