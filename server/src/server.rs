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
use distd_core::signature::{
    PublisherReleaseEnvelope, PublisherReleaseSequence, PublisherSignatureEnvelope,
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
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, RwLock};
use tracing::span;
use uuid::Uuid;

use crate::client::{Client, Name as ClientName};
use crate::error::Server as ServerError;
use crate::metadata_state::MetadataStateStore;
use crate::publication::{
    PublicationAttestation, PublicationError, PublicationRequestAttestation, PublishedRevision,
    PublisherPolicy,
};
use distd_core::feed::{Feed, Name as FeedName};
use distd_core::version::Version;

pub(crate) const DEFAULT_MAX_REGISTERED_CLIENTS: usize = 4_096;
pub(crate) const HARD_MAX_REGISTERED_CLIENTS: usize = 65_536;
pub(crate) const MAX_CLIENT_NAME_BYTES: usize = 256;

#[derive(Debug)]
struct PublicationLockSlot {
    lock: Arc<AsyncMutex<()>>,
    participants: usize,
}

struct PublicationLockLease {
    artifact_id: ArtifactId,
    lock: Arc<AsyncMutex<()>>,
    registry: Arc<StdMutex<HashMap<ArtifactId, PublicationLockSlot>>>,
}

impl Drop for PublicationLockLease {
    fn drop(&mut self) {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let remove = registry
            .get_mut(&self.artifact_id)
            .filter(|slot| Arc::ptr_eq(&slot.lock, &self.lock))
            .is_some_and(|slot| {
                slot.participants = slot
                    .participants
                    .checked_sub(1)
                    .expect("publication lock participant count underflow");
                slot.participants == 0
            });
        if remove {
            registry.remove(&self.artifact_id);
        }
    }
}

struct PublicationGuard {
    // Declaration order is part of the lifecycle: unlock before the lease may
    // remove the last registry slot.
    _guard: OwnedMutexGuard<()>,
    _lease: PublicationLockLease,
}

/// Data structure used internally by server, may be converted to `ServerMetadata`
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InternalMetadata {
    // TODO
    // server version
    pub version: Version,
    // Feed map
    pub feeds: HashMap<FeedName, Feed>,
    // Item map — keyed by ArtifactId
    pub items: HashMap<ArtifactId, PublishedRevision>,
    pub versions: HashMap<ArtifactId, BTreeMap<u64, PublishedRevision>>,
}

impl InternalMetadata {
    pub fn verify_storage<T>(&self, storage: &T) -> Result<(), ServerError>
    where
        T: ChunkStorage,
    {
        crate::metadata_state::validate_runtime_metadata(self)?;
        self.verify_storage_after_metadata_validation(storage)
    }

    pub(crate) fn verify_storage_after_metadata_validation<T>(
        &self,
        storage: &T,
    ) -> Result<(), ServerError>
    where
        T: ChunkStorage,
    {
        for revision in self.versions.values().flat_map(BTreeMap::values) {
            let item = revision.item();
            storage.verify_committed_revision(item).map_err(|error| {
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
            .flat_map(|versions| versions.values().map(|revision| revision.item().clone()))
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

fn validate_publisher_policy_markers(
    metadata: &InternalMetadata,
    policy: &PublisherPolicy,
) -> Result<(), ServerError> {
    if !policy.permits_unsigned() {
        for revision in metadata.versions.values().flat_map(BTreeMap::values) {
            match revision.attestation() {
                PublicationAttestation::ReleaseV2 { envelope, .. }
                    if policy.contains_exact_key(envelope.public_key()) => {}
                PublicationAttestation::ReleaseV2 { .. } => {
                    return Err(PublicationError::UntrustedPublisher.into());
                }
                PublicationAttestation::ReplayableV1(_) => {
                    return Err(PublicationError::ReplayableForbidden.into());
                }
                PublicationAttestation::UnsignedLegacy => {
                    return Err(PublicationError::UnsignedForbidden.into());
                }
            }
        }
    }
    Ok(())
}

impl From<InternalMetadata> for ServerMetadata {
    fn from(value: InternalMetadata) -> Self {
        Self {
            version: value.version,
            feeds: value.feeds,
            items: value
                .items
                .into_iter()
                .map(|(artifact_id, revision)| (artifact_id, revision.into_parts().0.metadata))
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
    publisher_policy: PublisherPolicy,

    /// global server metadata
    metadata: Arc<RwLock<InternalMetadata>>,
    /// A storage implementing `ChunkStorage`; owns its own concurrency internally.
    storage: Arc<T>,
    /// Client map
    pub clients: Arc<RwLock<BTreeMap<Uuid, Client>>>,
    max_registered_clients: usize,
    publish_locks: Arc<StdMutex<HashMap<ArtifactId, PublicationLockSlot>>>,
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
            publisher_policy: self.publisher_policy.clone(),
            metadata: Arc::clone(&self.metadata),
            storage: Arc::clone(&self.storage),
            clients: Arc::clone(&self.clients),
            max_registered_clients: self.max_registered_clients,
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
    fn publication_lock_stats_for_test(&self) -> (usize, usize) {
        let registry = self
            .publish_locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            registry.len(),
            registry.values().map(|slot| slot.participants).sum(),
        )
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
        expected_previous: Option<&PublishedRevision>,
        revision: PublishedRevision,
    ) -> Result<(), ServerError> {
        let live_metadata = Arc::clone(&self.metadata);
        let persist_lock = Arc::clone(&self.metadata_persist_lock);
        let store = self.metadata_store.clone();
        let publisher_policy = self.publisher_policy.clone();
        let expected_previous = expected_previous.cloned();
        tokio::task::spawn_blocking(move || {
            let _persist_guard = persist_lock
                .lock()
                .map_err(|_| ServerError::State("metadata commit lock is poisoned".to_string()))?;
            let snapshot = live_metadata.blocking_read();
            let mut next = (*snapshot).clone();
            let actual_previous = current_publication_revision(&next, &artifact_id)?;
            if actual_previous != expected_previous {
                return Err(ServerError::State(
                    "publication history changed before metadata commit".to_string(),
                ));
            }
            let versions = next.versions.entry(artifact_id.clone()).or_default();
            if versions.is_empty() {
                if let Some(previous) = actual_previous {
                    match versions.entry(previous.item().manifest.version) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(previous);
                        }
                        std::collections::btree_map::Entry::Occupied(_) => unreachable!(),
                    }
                }
            }
            match versions.entry(revision.item().manifest.version) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(revision.clone());
                }
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(ServerError::State(
                        "new publication conflicts with an existing immutable revision".to_string(),
                    ));
                }
            }
            next.items.insert(artifact_id, revision);
            validate_publisher_policy_markers(&next, &publisher_policy)?;
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
        revision: &PublishedRevision,
    ) -> Result<(), ServerError> {
        if self
            .metadata
            .read()
            .await
            .versions
            .get(artifact_id)
            .is_some_and(|versions| {
                versions.get(&revision.item().manifest.version) == Some(revision)
            })
        {
            return Ok(());
        }
        let live_metadata = Arc::clone(&self.metadata);
        let persist_lock = Arc::clone(&self.metadata_persist_lock);
        let store = self.metadata_store.clone();
        let publisher_policy = self.publisher_policy.clone();
        let artifact_id = artifact_id.to_string();
        let revision = revision.clone();
        tokio::task::spawn_blocking(move || {
            let _persist_guard = persist_lock
                .lock()
                .map_err(|_| ServerError::State("metadata commit lock is poisoned".to_string()))?;
            let snapshot = live_metadata.blocking_read();
            let mut next = (*snapshot).clone();
            if next.items.get(&artifact_id) != Some(&revision) {
                return Err(ServerError::State(
                    "publication changed before historical backfill".to_string(),
                ));
            }
            let versions = next.versions.entry(artifact_id).or_default();
            match versions.entry(revision.item().manifest.version) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(revision);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &revision => {
                }
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(ServerError::State(
                        "publication history conflicts during backfill".to_string(),
                    ));
                }
            }
            validate_publisher_policy_markers(&next, &publisher_policy)?;
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
            publisher_policy: PublisherPolicy::required([]),
            metadata: Arc::new(RwLock::new(metadata)),
            storage: Arc::new(storage),
            clients: Arc::new(RwLock::new(BTreeMap::new())),
            max_registered_clients: DEFAULT_MAX_REGISTERED_CLIENTS,
            publish_locks: Arc::new(StdMutex::new(HashMap::new())),
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
            publisher_policy: PublisherPolicy::required([]),
            metadata: Arc::new(RwLock::new(metadata)),
            storage: Arc::new(storage),
            clients: Arc::new(RwLock::new(BTreeMap::new())),
            max_registered_clients: DEFAULT_MAX_REGISTERED_CLIENTS,
            publish_locks: Arc::new(StdMutex::new(HashMap::new())),
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

    /// Attach the immutable publisher policy used for publication and sync.
    #[must_use]
    pub fn with_publisher_policy(mut self, policy: PublisherPolicy) -> Self {
        self.publisher_policy = policy;
        self
    }

    #[must_use]
    pub(crate) fn with_client_registry_limit(mut self, max_registered_clients: usize) -> Self {
        assert!((1..=HARD_MAX_REGISTERED_CLIENTS).contains(&max_registered_clients));
        self.max_registered_clients = max_registered_clients;
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

    async fn publication_guard(&self, artifact_id: &str) -> Result<PublicationGuard, ServerError> {
        let lease = {
            let mut registry = self.publish_locks.lock().map_err(|_| {
                ServerError::State("publication lock registry is poisoned".to_string())
            })?;
            let slot =
                registry
                    .entry(artifact_id.to_string())
                    .or_insert_with(|| PublicationLockSlot {
                        lock: Arc::new(AsyncMutex::new(())),
                        participants: 0,
                    });
            slot.participants = slot.participants.checked_add(1).ok_or_else(|| {
                ServerError::State("publication lock participant count is exhausted".to_string())
            })?;
            PublicationLockLease {
                artifact_id: artifact_id.to_string(),
                lock: Arc::clone(&slot.lock),
                registry: Arc::clone(&self.publish_locks),
            }
        };
        let guard = Arc::clone(&lease.lock).lock_owned().await;
        Ok(PublicationGuard {
            _guard: guard,
            _lease: lease,
        })
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
        let metadata = Arc::clone(&self.metadata);
        let publisher_policy = self.publisher_policy.clone();
        tokio::task::spawn_blocking(move || {
            let artifact_id = manifest_req.artifact_id;
            let revision = {
                let metadata = metadata.blocking_read();
                match manifest_req.version {
                    Some(version) => metadata
                        .versions
                        .get(&artifact_id)
                        .and_then(|versions| versions.get(&version))
                        .cloned(),
                    None => metadata.items.get(&artifact_id).cloned(),
                }
            }
            .ok_or_else(|| format!("Unknown artifact: {artifact_id}"))?;
            publisher_policy
                .validate_metadata_compatibility(&revision)
                .map_err(|error| error.to_string())?;
            let (item, attestation) = revision.into_parts();
            let (
                publisher_public_key,
                publisher_signature,
                publisher_statement_version,
                publisher_release_sequence,
            ) = match attestation {
                PublicationAttestation::ReleaseV2 { sequence, envelope } => (
                    envelope.public_key().as_bytes().to_vec(),
                    envelope.signature().as_bytes().to_vec(),
                    Some(2),
                    Some(sequence.get()),
                ),
                PublicationAttestation::ReplayableV1(envelope) => (
                    envelope.public_key().as_bytes().to_vec(),
                    envelope.signature().as_bytes().to_vec(),
                    Some(1),
                    None,
                ),
                PublicationAttestation::UnsignedLegacy => (Vec::new(), Vec::new(), None, None),
            };
            let chunk_sizes = item
                .chunks
                .iter()
                .map(|chunk| {
                    u32::try_from(chunk.size)
                        .map_err(|_| "signed manifest chunk size exceeds wire limit".to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            let response = proto::ManifestResponse {
                artifact_id: item.manifest.artifact_id.clone(),
                version: item.manifest.version,
                root_hash: item.root().as_bytes().to_vec(),
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
                chunk_sizes,
                publisher_public_key,
                publisher_signature,
                publisher_statement_version,
                publisher_release_sequence,
            };
            Ok::<_, String>((response, item))
        })
        .await
        .map_err(|error| format!("manifest response worker failed: {error}"))?
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
        if name.is_empty() || name.len() > MAX_CLIENT_NAME_BYTES {
            tracing::warn!(reason = "invalid_name", "client registration rejected");
            return Err(RegisterError);
        }

        let span = span!(tracing::Level::INFO, "register_client");
        let _entered = span.enter();

        if let Some(u) = uuid {
            let mut clients = self.clients.write().await;
            if let Some(existing) = clients.get_mut(&u) {
                existing.last_heartbeat = SystemTime::now();
                tracing::info!("existing client registration refreshed");
                Ok(u)
            } else {
                tracing::warn!(reason = "unknown_uuid", "client registration rejected");
                Err(RegisterError)
            }
        } else {
            let nonced_name = name.clone() + &self.uuid_nonce + &addr.to_string();
            let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, nonced_name.as_bytes());
            let client = Client {
                addr,
                name,
                uuid,
                version,
                last_heartbeat: SystemTime::now(),
            };
            let mut clients = self.clients.write().await;
            if let Some(existing) = clients.get_mut(&uuid) {
                if existing.name != client.name || existing.addr != client.addr {
                    tracing::warn!(
                        reason = "identity_collision",
                        "client registration rejected"
                    );
                    return Err(RegisterError);
                }
                existing.last_heartbeat = SystemTime::now();
                tracing::info!("derived client registration refreshed");
                return Ok(uuid);
            }
            if clients.len() >= self.max_registered_clients {
                tracing::warn!(reason = "registry_full", "client registration rejected");
                return Err(RegisterError);
            }
            clients.insert(uuid, client);
            tracing::info!("new client registration accepted");
            Ok(uuid)
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
        let publisher_policy = self.publisher_policy.clone();
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
            validate_publisher_policy_markers(&next, &publisher_policy)?;
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
        self.publish_item_as_with_attestation(
            artifact_id,
            name,
            path,
            description,
            file,
            chunk_algorithm,
            PublicationRequestAttestation::Unsigned,
        )
        .await
    }

    /// Publish a replayable V1 revision in explicit compatibility mode.
    pub async fn publish_signed_item_as(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        file: Bytes,
        chunk_algorithm: ChunkAlgorithm,
        envelope: PublisherSignatureEnvelope,
    ) -> Result<Item, ServerError>
    where
        T: 'static,
    {
        self.publish_item_as_with_attestation(
            artifact_id,
            name,
            path,
            description,
            file,
            chunk_algorithm,
            PublicationRequestAttestation::ReplayableV1(envelope),
        )
        .await
    }

    /// Publish a rollback-protected V2 publisher release.
    pub async fn publish_release_item_as(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        file: Bytes,
        chunk_algorithm: ChunkAlgorithm,
        sequence: PublisherReleaseSequence,
        envelope: PublisherReleaseEnvelope,
    ) -> Result<Item, ServerError>
    where
        T: 'static,
    {
        self.publish_item_as_with_attestation(
            artifact_id,
            name,
            path,
            description,
            file,
            chunk_algorithm,
            PublicationRequestAttestation::ReleaseV2 { sequence, envelope },
        )
        .await
    }

    async fn publish_item_as_with_attestation(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        file: Bytes,
        chunk_algorithm: ChunkAlgorithm,
        attestation: PublicationRequestAttestation,
    ) -> Result<Item, ServerError>
    where
        T: 'static,
    {
        self.precheck_publication_attestation(&attestation)?;
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
                    attestation,
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
        attestation: PublicationRequestAttestation,
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
        let _publish_guard = self.publication_guard(&artifact_id).await?;
        self.require_publication_writable()?;
        let policy = self.publisher_policy.clone();
        let authorized =
            tokio::task::spawn_blocking(move || policy.authorize_request(attestation, prepared))
                .await
                .map_err(|error| {
                    ServerError::State(format!("publisher verification worker failed: {error}"))
                })??;
        let previous = {
            let metadata = self.metadata.read().await;
            current_publication_revision(&metadata, &artifact_id)?
        };
        if let Some(old) = previous.as_ref() {
            if same_signed_statement_content(old.item(), authorized.item())
                && old.attestation() != authorized.attestation()
            {
                return Err(PublicationError::AttestationConflict.into());
            }
            if same_version_relevant_metadata(old.item(), authorized.item())
                && old.attestation() == authorized.attestation()
            {
                self.ensure_publication_history(&artifact_id, old).await?;
                return Ok(old.item().clone());
            }
        }
        let revision = previous.as_ref().map_or(Ok(0), |item| {
            item.item()
                .metadata
                .revision
                .checked_add(1)
                .ok_or_else(|| ServerError::State("artifact revision is exhausted".to_string()))
        })?;
        let (prepared, attestation) = authorized.into_parts();
        let storage = Arc::clone(&self.storage);
        let (published, item) = tokio::task::spawn_blocking(move || {
            storage
                .commit_revision(prepared.with_revision(revision), file)
                .map(|item| {
                    let returned = item.clone();
                    (
                        PublishedRevision::from_verified_parts(item, attestation),
                        returned,
                    )
                })
        })
        .await
        .map_err(|error| ServerError::State(format!("revision storage worker failed: {error}")))?
        .map_err(|error| {
            tracing::error!("Storage error: {error}");
            ServerError::ChunkInsertError
        })?;

        if let Err(error) = self
            .commit_publication_metadata(artifact_id, previous.as_ref(), published)
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
        self.publish_item_from_files_as_with_attestation(
            artifact_id,
            name,
            path,
            description,
            files,
            chunk_algorithm,
            PublicationRequestAttestation::Unsigned,
        )
        .await
    }

    /// Publish a replayable V1 directory revision in compatibility mode.
    pub async fn publish_signed_item_from_files_as(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        files: Vec<(PathBuf, Bytes)>,
        chunk_algorithm: ChunkAlgorithm,
        envelope: PublisherSignatureEnvelope,
    ) -> Result<Item, ServerError>
    where
        T: 'static,
    {
        self.publish_item_from_files_as_with_attestation(
            artifact_id,
            name,
            path,
            description,
            files,
            chunk_algorithm,
            PublicationRequestAttestation::ReplayableV1(envelope),
        )
        .await
    }

    /// Publish a rollback-protected V2 directory release.
    pub async fn publish_release_item_from_files_as(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        files: Vec<(PathBuf, Bytes)>,
        chunk_algorithm: ChunkAlgorithm,
        sequence: PublisherReleaseSequence,
        envelope: PublisherReleaseEnvelope,
    ) -> Result<Item, ServerError>
    where
        T: 'static,
    {
        self.publish_item_from_files_as_with_attestation(
            artifact_id,
            name,
            path,
            description,
            files,
            chunk_algorithm,
            PublicationRequestAttestation::ReleaseV2 { sequence, envelope },
        )
        .await
    }

    async fn publish_item_from_files_as_with_attestation(
        &self,
        artifact_id: ArtifactId,
        name: ItemName,
        path: PathBuf,
        description: Option<String>,
        files: Vec<(PathBuf, Bytes)>,
        chunk_algorithm: ChunkAlgorithm,
        attestation: PublicationRequestAttestation,
    ) -> Result<Item, ServerError>
    where
        T: 'static,
    {
        self.precheck_publication_attestation(&attestation)?;
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
                    attestation,
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
        files: Vec<(PathBuf, Bytes)>,
        chunk_algorithm: ChunkAlgorithm,
        attestation: PublicationRequestAttestation,
    ) -> Result<Item, ServerError> {
        let storage = Arc::clone(&self.storage);
        let prepare_artifact_id = artifact_id.clone();
        let (prepared, files) = tokio::task::spawn_blocking(move || {
            let mut files = files;
            files.sort_by(|left, right| {
                left.0
                    .to_str()
                    .unwrap_or_default()
                    .as_bytes()
                    .cmp(right.0.to_str().unwrap_or_default().as_bytes())
            });
            storage
                .prepare_revision_from_sorted_files(
                    prepare_artifact_id,
                    name,
                    path,
                    0,
                    description,
                    &files,
                    chunk_algorithm,
                )
                .map(|prepared| (prepared, files))
        })
        .await
        .map_err(|error| {
            ServerError::State(format!("directory revision prepare worker failed: {error}"))
        })?
        .map_err(|error| {
            tracing::error!("Cannot prepare directory revision: {error}");
            ServerError::ChunkInsertError
        })?;

        let _publish_guard = self.publication_guard(&artifact_id).await?;
        self.require_publication_writable()?;
        let policy = self.publisher_policy.clone();
        let authorized =
            tokio::task::spawn_blocking(move || policy.authorize_request(attestation, prepared))
                .await
                .map_err(|error| {
                    ServerError::State(format!("publisher verification worker failed: {error}"))
                })??;
        let previous = {
            let metadata = self.metadata.read().await;
            current_publication_revision(&metadata, &artifact_id)?
        };
        if let Some(old) = previous.as_ref() {
            if same_signed_statement_content(old.item(), authorized.item())
                && old.attestation() != authorized.attestation()
            {
                return Err(PublicationError::AttestationConflict.into());
            }
            if same_version_relevant_metadata(old.item(), authorized.item())
                && old.attestation() == authorized.attestation()
            {
                self.ensure_publication_history(&artifact_id, old).await?;
                return Ok(old.item().clone());
            }
        }
        let revision = previous.as_ref().map_or(Ok(0), |item| {
            item.item()
                .metadata
                .revision
                .checked_add(1)
                .ok_or_else(|| ServerError::State("artifact revision is exhausted".to_string()))
        })?;
        let (prepared, attestation) = authorized.into_parts();
        let storage = Arc::clone(&self.storage);
        let (published, item) = tokio::task::spawn_blocking(move || {
            storage
                .commit_revision_from_files(prepared.with_revision(revision), files)
                .map(|item| {
                    let returned = item.clone();
                    (
                        PublishedRevision::from_verified_parts(item, attestation),
                        returned,
                    )
                })
        })
        .await
        .map_err(|error| ServerError::State(format!("revision storage worker failed: {error}")))?
        .map_err(|error| {
            tracing::error!("Storage error: {error}");
            ServerError::ChunkInsertError
        })?;

        if let Err(error) = self
            .commit_publication_metadata(artifact_id, previous.as_ref(), published)
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

    pub(crate) fn precheck_publication_attestation(
        &self,
        attestation: &PublicationRequestAttestation,
    ) -> Result<(), ServerError> {
        self.publisher_policy
            .precheck_request(attestation)
            .map_err(Into::into)
    }

    /// Get the public key of the server
    #[must_use]
    pub fn public_key(&self) -> &[u8] {
        self.key_pair.public_key().as_ref()
    }
}

fn current_publication_revision(
    metadata: &InternalMetadata,
    artifact_id: &str,
) -> Result<Option<PublishedRevision>, ServerError> {
    let current = metadata.items.get(artifact_id).cloned();
    if let Some(current) = &current {
        validate_item_identity(artifact_id, current.item())?;
    }
    let historical = if let Some(versions) = metadata.versions.get(artifact_id) {
        for (version, revision) in versions {
            validate_item_identity(artifact_id, revision.item())?;
            if *version != revision.item().manifest.version {
                return Err(ServerError::State(format!(
                    "artifact {artifact_id} history key disagrees with its item version"
                )));
            }
        }
        versions
            .last_key_value()
            .map(|(version, revision)| (*version, revision.clone()))
    } else {
        None
    };
    match (&current, &historical) {
        (None, None) => Ok(None),
        (Some(current), Some((version, historical)))
            if current == historical && *version == current.item().manifest.version =>
        {
            Ok(current.clone().into())
        }
        (Some(current), None) => Ok(current.clone().into()),
        (None, Some(_)) | (Some(_), Some(_)) => Err(ServerError::State(format!(
            "artifact {artifact_id} current revision disagrees with immutable history"
        ))),
    }
}

fn same_signed_statement_content(left: &Item, right: &Item) -> bool {
    left.manifest.artifact_id == right.manifest.artifact_id
        && left.manifest.root_hash == right.manifest.root_hash
        && left.manifest.total_size == right.manifest.total_size
        && left.manifest.chunk_algorithm == right.manifest.chunk_algorithm
        && left.manifest.entries == right.manifest.entries
        && left.chunks == right.chunks
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
    use super::{
        build_sync_payload_batches, InternalMetadata, Server as ProductionServer, ServerError,
        DEFAULT_MAX_REGISTERED_CLIENTS, MAX_CLIENT_NAME_BYTES,
    };
    use axum::body::Bytes;
    use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
    use distd_core::chunk_storage::ChunkStorage;
    use distd_core::chunks::ChunkAlgorithm;
    use distd_core::durable_state::{
        replace_enveloped_state, DurableReplaceStage, DurableStateError, DurableStateKind,
    };
    use distd_core::item::Item;
    use distd_core::proto::ManifestRequest;
    use distd_core::proto::{sync_message::Msg, PayloadCompression};
    use prost::Message as _;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    use crate::publication::{PublicationError, PublishedRevision, PublisherPolicy};
    use distd_core::signature::{
        PublisherPublicKey, PublisherReleaseEnvelope, PublisherReleaseSequence,
        PublisherReleaseStatement, PublisherSignature,
    };
    use ring::signature::{Ed25519KeyPair, KeyPair};

    fn unsigned(item: Item) -> PublishedRevision {
        PublishedRevision::unsigned(item)
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

    #[tokio::test]
    async fn client_registry_bounds_names_and_keeps_exact_reuse_at_capacity() {
        let server = ProductionServer::new_ephemeral(HashMapStorage::default())
            .with_client_registry_limit(2);
        let address = "127.0.0.1:41001".parse().unwrap();

        assert!(server
            .register_client(String::new(), address, None, None)
            .await
            .is_err());
        assert!(server
            .register_client("x".repeat(MAX_CLIENT_NAME_BYTES + 1), address, None, None)
            .await
            .is_err());
        assert!(server.clients.read().await.is_empty());

        let first = server
            .register_client("a".to_string(), address, None, None)
            .await
            .unwrap();
        let second = server
            .register_client("b".repeat(MAX_CLIENT_NAME_BYTES), address, None, None)
            .await
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(server.clients.read().await.len(), 2);

        assert!(server
            .register_client("c".to_string(), address, None, None)
            .await
            .is_err());
        assert_eq!(server.clients.read().await.len(), 2);
        assert_eq!(
            server
                .register_client("a".to_string(), address, None, Some(first))
                .await
                .unwrap(),
            first
        );
        assert_eq!(
            server
                .register_client("a".to_string(), address, None, None)
                .await
                .unwrap(),
            first
        );
        assert_eq!(server.clients.read().await.len(), 2);
    }

    #[tokio::test]
    async fn competing_registrations_never_exceed_the_final_registry_slot() {
        let server = ProductionServer::new_ephemeral(HashMapStorage::default())
            .with_client_registry_limit(1);
        let address = "127.0.0.1:41002".parse().unwrap();
        let left = server.clone();
        let right = server.clone();
        let (left, right) = tokio::join!(
            left.register_client("left".to_string(), address, None, None),
            right.register_client("right".to_string(), address, None, None)
        );
        assert_eq!(usize::from(left.is_ok()) + usize::from(right.is_ok()), 1);
        assert_eq!(server.clients.read().await.len(), 1);
    }

    #[tokio::test]
    async fn publication_lock_registry_releases_distinct_success_and_rejection_slots() {
        let server = ProductionServer::new_ephemeral(HashMapStorage::default());
        for index in 0..10_000 {
            let artifact_id = format!("ephemeral-{index}");
            let guard = server.publication_guard(&artifact_id).await.unwrap();
            assert_eq!(server.publication_lock_stats_for_test(), (1, 1));
            if index % 2 == 0 {
                drop(guard);
            } else {
                let rejected: Result<(), PublicationError> = {
                    let _guard = guard;
                    Err(PublicationError::UntrustedPublisher)
                };
                assert_eq!(rejected, Err(PublicationError::UntrustedPublisher));
            }
            assert_eq!(server.publication_lock_stats_for_test(), (0, 0));
        }
    }

    #[tokio::test]
    async fn cancelled_publication_waiter_releases_only_its_participation() {
        let server = ProductionServer::new_ephemeral(HashMapStorage::default());
        let active = server.publication_guard("shared").await.unwrap();
        assert_eq!(server.publication_lock_stats_for_test(), (1, 1));

        let waiter_server = server.clone();
        let waiter = tokio::spawn(async move { waiter_server.publication_guard("shared").await });
        for _ in 0..100 {
            if server.publication_lock_stats_for_test() == (1, 2) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(server.publication_lock_stats_for_test(), (1, 2));

        waiter.abort();
        let cancellation = waiter.await;
        assert!(cancellation
            .as_ref()
            .is_err_and(tokio::task::JoinError::is_cancelled));
        assert_eq!(server.publication_lock_stats_for_test(), (1, 1));
        drop(active);
        assert_eq!(server.publication_lock_stats_for_test(), (0, 0));
    }

    #[tokio::test]
    #[ignore = "release-only local publication-lock registry reference"]
    async fn publication_lock_registry_reference_performance() {
        assert!(
            !cfg!(debug_assertions),
            "run the publication-lock registry reference in release mode"
        );
        let server = ProductionServer::new_ephemeral(HashMapStorage::default());
        let mut samples = Vec::with_capacity(20_000);
        for index in 0..20_000 {
            let artifact_id = format!("reference-{index}");
            let started = std::time::Instant::now();
            let guard = server.publication_guard(&artifact_id).await.unwrap();
            drop(guard);
            samples.push(started.elapsed().as_nanos());
        }
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let p95 = sorted[(sorted.len() * 95).div_ceil(100) - 1];
        println!("publication_lock_registry_raw_ns={samples:?}");
        println!("publication_lock_registry_p95_ns={p95}");
        assert!(p95 <= 10_000, "publication-lock registry p95 {p95}ns");
        assert_eq!(server.publication_lock_stats_for_test(), (0, 0));
    }

    #[tokio::test]
    #[ignore = "release-only local client-registry reference"]
    async fn client_registry_reference_performance() {
        assert!(
            !cfg!(debug_assertions),
            "run the client registry reference in release mode"
        );
        let server = ProductionServer::new_ephemeral(HashMapStorage::default())
            .with_client_registry_limit(DEFAULT_MAX_REGISTERED_CLIENTS);
        let address = "127.0.0.1:41003".parse().unwrap();
        let mut first = None;
        for index in 0..DEFAULT_MAX_REGISTERED_CLIENTS {
            let uuid = server
                .register_client(format!("client-{index}"), address, None, None)
                .await
                .unwrap();
            first.get_or_insert(uuid);
        }
        let first = first.unwrap();

        let mut reuse_samples = Vec::with_capacity(10_000);
        let mut reject_samples = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            let started = std::time::Instant::now();
            assert_eq!(
                server
                    .register_client("client-0".to_string(), address, None, Some(first))
                    .await
                    .unwrap(),
                first
            );
            reuse_samples.push(started.elapsed().as_nanos());

            let started = std::time::Instant::now();
            assert!(server
                .register_client("registry-full".to_string(), address, None, None)
                .await
                .is_err());
            reject_samples.push(started.elapsed().as_nanos());
        }
        let percentile = |samples: &[u128]| {
            let mut sorted = samples.to_vec();
            sorted.sort_unstable();
            sorted[(sorted.len() * 95).div_ceil(100) - 1]
        };
        let reuse_p95 = percentile(&reuse_samples);
        let reject_p95 = percentile(&reject_samples);
        println!("client_registry_reuse_raw_ns={reuse_samples:?}");
        println!("client_registry_reject_raw_ns={reject_samples:?}");
        println!(
            "client_registry_reuse_p95_ns={reuse_p95} client_registry_reject_p95_ns={reject_p95}"
        );
        assert!(reuse_p95 <= 100_000, "registry reuse p95 {reuse_p95}ns");
        assert!(
            reject_p95 <= 100_000,
            "registry rejection p95 {reject_p95}ns"
        );
        assert_eq!(
            server.clients.read().await.len(),
            DEFAULT_MAX_REGISTERED_CLIENTS
        );
    }

    #[test]
    fn publisher_release_wire_tuple_has_exact_bounded_delta() {
        let mut key_and_signature = distd_core::proto::ManifestResponse::default();
        assert!(key_and_signature.encode_to_vec().is_empty());

        key_and_signature.publisher_public_key = vec![0x11; 32];
        key_and_signature.publisher_signature = vec![0x22; 64];
        let key_and_signature_bytes = key_and_signature.encode_to_vec();
        assert_eq!(key_and_signature_bytes.len(), 100);

        let mut v1 = key_and_signature.clone();
        v1.publisher_statement_version = Some(1);
        assert_eq!(v1.encode_to_vec().len(), 102);

        let mut v2 = key_and_signature;
        v2.publisher_statement_version = Some(2);
        v2.publisher_release_sequence = Some(1);
        let v2_bytes = v2.encode_to_vec();
        assert_eq!(v2_bytes.len(), 104);
        assert_eq!(&v2_bytes[..100], key_and_signature_bytes.as_slice());
        assert_eq!(&v2_bytes[100..], &[0x68, 0x02, 0x70, 0x01]);

        v2.publisher_release_sequence = Some(u64::MAX);
        let maximum_bytes = v2.encode_to_vec();
        assert_eq!(maximum_bytes.len(), 113);
        assert_eq!(&maximum_bytes[100..103], &[0x68, 0x02, 0x70]);
        assert_eq!(
            &maximum_bytes[103..],
            &[0xff; 9].into_iter().chain([0x01]).collect::<Vec<_>>()
        );
    }

    #[derive(Clone, Debug, Default)]
    struct CountingStorage {
        inner: HashMapStorage,
        commits: Arc<AtomicUsize>,
    }

    impl
        distd_core::hash::HashTreeCapable<
            Arc<distd_core::chunk_storage::Node>,
            distd_core::error::Error,
        > for CountingStorage
    {
        fn func(
            &self,
            data: &[u8],
        ) -> Result<Arc<distd_core::chunk_storage::Node>, distd_core::error::Error> {
            self.inner.func(data)
        }

        fn merge(
            &self,
            left: &Arc<distd_core::chunk_storage::Node>,
            right: &Arc<distd_core::chunk_storage::Node>,
        ) -> Result<Arc<distd_core::chunk_storage::Node>, distd_core::error::Error> {
            self.inner.merge(left, right)
        }
    }

    impl ChunkStorage for CountingStorage {
        fn get(
            &self,
            hash: &distd_core::hash::Hash,
        ) -> Option<Arc<distd_core::chunk_storage::Node>> {
            self.inner.get(hash)
        }

        fn store_chunk(
            &self,
            hash: distd_core::hash::Hash,
            chunk: &[u8],
        ) -> Result<Arc<distd_core::chunk_storage::Node>, distd_core::chunk_storage::StorageError>
        {
            self.inner.store_chunk(hash, chunk)
        }

        fn store_link(
            &self,
            hash: distd_core::hash::Hash,
            left: Arc<distd_core::chunk_storage::Node>,
            right: Arc<distd_core::chunk_storage::Node>,
        ) -> Result<Arc<distd_core::chunk_storage::Node>, distd_core::chunk_storage::StorageError>
        {
            self.inner.store_link(hash, left, right)
        }

        fn chunks(&self) -> Vec<distd_core::hash::Hash> {
            self.inner.chunks()
        }

        fn size(&self) -> u64 {
            self.inner.size()
        }

        fn commit_revision(
            &self,
            item: Item,
            file: Bytes,
        ) -> Result<Item, distd_core::error::Error> {
            self.commits.fetch_add(1, Ordering::Relaxed);
            self.inner.commit_revision(item, file)
        }

        fn commit_revision_from_files(
            &self,
            item: Item,
            files: Vec<(std::path::PathBuf, Bytes)>,
        ) -> Result<Item, distd_core::error::Error> {
            self.commits.fetch_add(1, Ordering::Relaxed);
            self.inner.commit_revision_from_files(item, files)
        }
    }

    #[derive(Debug, Default)]
    struct CountingMetadataReplace {
        calls: AtomicUsize,
    }

    impl crate::metadata_state::MetadataReplace for CountingMetadataReplace {
        fn replace(&self, path: &Path, payload: &[u8]) -> Result<(), DurableStateError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            replace_enveloped_state(
                path,
                DurableStateKind::ServerPublication,
                crate::metadata_state::SERVER_PUBLICATION_SCHEMA,
                payload,
                crate::metadata_state::MAX_SERVER_PUBLICATION_PAYLOAD,
            )
        }
    }

    struct Server;

    impl Server {
        fn new_ephemeral<T>(storage: T) -> ProductionServer<T>
        where
            T: ChunkStorage + Sync + Send + std::fmt::Debug + 'static,
        {
            ProductionServer::new_ephemeral(storage)
                .with_publisher_policy(PublisherPolicy::explicitly_insecure_unsigned())
        }

        fn new_ephemeral_with_metadata<T>(
            storage: T,
            metadata: InternalMetadata,
        ) -> ProductionServer<T>
        where
            T: ChunkStorage + Sync + Send + std::fmt::Debug + 'static,
        {
            ProductionServer::new_ephemeral_with_metadata(storage, metadata)
                .with_publisher_policy(PublisherPolicy::explicitly_insecure_unsigned())
        }
    }

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
    async fn signed_publication_verifies_before_commit_and_is_exactly_idempotent() {
        let storage = CountingStorage::default();
        let storage_commits = Arc::clone(&storage.commits);
        let bytes = Bytes::from_static(b"publisher-authenticated revision");
        let algorithm = ChunkAlgorithm::Fixed256K;
        let prepared = storage
            .prepare_revision(
                "signed-id".to_string(),
                "display".to_string(),
                "signed.bin".into(),
                0,
                None,
                bytes.clone(),
                algorithm,
            )
            .unwrap();
        let (sequence, envelope) = signed_release(&prepared, [7; 32], 1);
        let policy = PublisherPolicy::required([envelope.public_key()]);
        let directory =
            std::env::temp_dir().join(format!("distd-signed-idempotence-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let replacer = Arc::new(CountingMetadataReplace::default());
        let (store, metadata) = crate::metadata_state::MetadataStateStore::open_with_replacer(
            paths.clone(),
            replacer.clone(),
            |_| Ok(()),
        )
        .unwrap();
        let bootstrap_writes = replacer.calls.load(Ordering::Relaxed);
        let server = ProductionServer::new_ephemeral_with_metadata(storage, metadata)
            .with_publisher_policy(policy.clone())
            .with_metadata_store(store);

        let first = server
            .publish_release_item_as(
                "signed-id".to_string(),
                "display".to_string(),
                "signed.bin".into(),
                None,
                bytes.clone(),
                algorithm,
                sequence,
                envelope,
            )
            .await
            .unwrap();
        let size_after_first = server.storage().size();
        assert_eq!(storage_commits.load(Ordering::Relaxed), 1);
        let metadata_writes_after_first = replacer.calls.load(Ordering::Relaxed);
        assert_eq!(metadata_writes_after_first, bootstrap_writes + 1);
        let duplicate = server
            .publish_release_item_as(
                "signed-id".to_string(),
                "display".to_string(),
                "signed.bin".into(),
                None,
                bytes,
                algorithm,
                sequence,
                envelope,
            )
            .await
            .unwrap();
        assert_eq!(duplicate, first);
        assert_eq!(server.storage().size(), size_after_first);
        assert_eq!(storage_commits.load(Ordering::Relaxed), 1);
        assert_eq!(
            replacer.calls.load(Ordering::Relaxed),
            metadata_writes_after_first
        );
        let metadata = server.metadata_snapshot().await;
        let history = metadata.versions.get("signed-id").unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(
            history.get(&0).unwrap().attestation().release(),
            Some((sequence, &envelope))
        );

        let (response, exact) = server
            .sync_manifest_response(ManifestRequest {
                artifact_id: "signed-id".to_string(),
                version: Some(0),
            })
            .await
            .unwrap();
        assert_eq!(exact, first);
        assert_eq!(
            response.publisher_public_key,
            envelope.public_key().as_bytes()
        );
        assert_eq!(
            response.publisher_signature,
            envelope.signature().as_bytes()
        );
        assert_eq!(response.publisher_statement_version, Some(2));
        assert_eq!(response.publisher_release_sequence, Some(sequence.get()));
        let (_, restarted) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |candidate| {
                crate::metadata_state::validate_runtime_metadata_with_policy(candidate, &policy)
            })
            .unwrap();
        assert_eq!(restarted.items.get("signed-id").unwrap().item(), &first);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn invalid_or_unknown_publisher_never_mutates_storage_or_metadata() {
        let storage = HashMapStorage::default();
        let bytes = Bytes::from_static(b"rejected signed revision");
        let prepared = storage
            .prepare_revision(
                "rejected".to_string(),
                "rejected".to_string(),
                "rejected.bin".into(),
                0,
                None,
                bytes.clone(),
                ChunkAlgorithm::Fixed256K,
            )
            .unwrap();
        let (sequence, allowed) = signed_release(&prepared, [11; 32], 1);
        let (_, unknown) = signed_release(&prepared, [13; 32], 1);
        let directory = std::env::temp_dir().join(format!(
            "distd-invalid-signed-publication-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&directory).unwrap();
        let paths = crate::metadata_state::MetadataPaths::in_state_directory(&directory);
        let (store, metadata) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths.clone(), |_| {
                Ok(())
            })
            .unwrap();
        let durable_before = std::fs::read(&paths.current).unwrap();
        let server = ProductionServer::new_ephemeral_with_metadata(storage, metadata)
            .with_publisher_policy(PublisherPolicy::required([allowed.public_key()]))
            .with_metadata_store(store);
        let before = server.metadata_snapshot().await;
        let size_before = server.storage().size();

        let missing_error = server
            .publish_item_as(
                "rejected".to_string(),
                "rejected".to_string(),
                "rejected.bin".into(),
                None,
                bytes.clone(),
                ChunkAlgorithm::Fixed256K,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            missing_error,
            ServerError::Publication(PublicationError::UnsignedForbidden)
        ));

        let unknown_error = server
            .publish_release_item_as(
                "rejected".to_string(),
                "rejected".to_string(),
                "rejected.bin".into(),
                None,
                bytes.clone(),
                ChunkAlgorithm::Fixed256K,
                sequence,
                unknown,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            unknown_error,
            ServerError::Publication(PublicationError::UntrustedPublisher)
        ));

        let mut invalid_signature = *allowed.signature().as_bytes();
        invalid_signature[0] ^= 1;
        let invalid = PublisherReleaseEnvelope::from_parts(
            allowed.public_key(),
            PublisherSignature::from_bytes(invalid_signature),
        );
        let invalid_error = server
            .publish_release_item_as(
                "rejected".to_string(),
                "rejected".to_string(),
                "rejected.bin".into(),
                None,
                bytes,
                ChunkAlgorithm::Fixed256K,
                sequence,
                invalid,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            invalid_error,
            ServerError::Publication(PublicationError::InvalidSignature(_))
        ));
        let cross_artifact = server
            .publish_release_item_as(
                "other-artifact".to_string(),
                "rejected".to_string(),
                "rejected.bin".into(),
                None,
                Bytes::from_static(b"rejected signed revision"),
                ChunkAlgorithm::Fixed256K,
                sequence,
                allowed,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            cross_artifact,
            ServerError::Publication(PublicationError::InvalidSignature(_))
        ));
        assert_eq!(server.metadata_snapshot().await, before);
        assert_eq!(server.storage().size(), size_before);
        assert_eq!(std::fs::read(&paths.current).unwrap(), durable_before);
        drop(server);
        let (_, restarted) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |_| Ok(()))
                .unwrap();
        assert_eq!(restarted, before);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn signed_history_keeps_each_exact_envelope_and_rejects_key_substitution() {
        let storage = HashMapStorage::default();
        let first_bytes = Bytes::from_static(b"first signed revision");
        let second_bytes = Bytes::from_static(b"second signed revision");
        let prepare = |bytes: Bytes| {
            storage
                .prepare_revision(
                    "rotating".to_string(),
                    "rotating".to_string(),
                    "rotating.bin".into(),
                    0,
                    None,
                    bytes,
                    ChunkAlgorithm::Fixed256K,
                )
                .unwrap()
        };
        let (first_sequence, first_envelope) =
            signed_release(&prepare(first_bytes.clone()), [17; 32], 1);
        let (_, substituted) = signed_release(&prepare(first_bytes.clone()), [19; 32], 1);
        let (second_sequence, second_envelope) =
            signed_release(&prepare(second_bytes.clone()), [19; 32], 2);
        let server = ProductionServer::new_ephemeral(storage).with_publisher_policy(
            PublisherPolicy::required([first_envelope.public_key(), second_envelope.public_key()]),
        );

        let first = server
            .publish_release_item_as(
                "rotating".to_string(),
                "rotating".to_string(),
                "rotating.bin".into(),
                None,
                first_bytes.clone(),
                ChunkAlgorithm::Fixed256K,
                first_sequence,
                first_envelope,
            )
            .await
            .unwrap();
        let conflict = server
            .publish_release_item_as(
                "rotating".to_string(),
                "rotating".to_string(),
                "rotating.bin".into(),
                None,
                first_bytes,
                ChunkAlgorithm::Fixed256K,
                first_sequence,
                substituted,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            conflict,
            ServerError::Publication(PublicationError::AttestationConflict)
        ));

        let second = server
            .publish_release_item_as(
                "rotating".to_string(),
                "rotating".to_string(),
                "rotating.bin".into(),
                None,
                second_bytes,
                ChunkAlgorithm::Fixed256K,
                second_sequence,
                second_envelope,
            )
            .await
            .unwrap();
        assert_eq!(second.metadata.revision, first.metadata.revision + 1);
        for (version, sequence, envelope) in [
            (0, first_sequence, first_envelope),
            (1, second_sequence, second_envelope),
        ] {
            let (response, _) = server
                .sync_manifest_response(ManifestRequest {
                    artifact_id: "rotating".to_string(),
                    version: Some(version),
                })
                .await
                .unwrap();
            assert_eq!(
                response.publisher_public_key,
                envelope.public_key().as_bytes()
            );
            assert_eq!(
                response.publisher_signature,
                envelope.signature().as_bytes()
            );
            assert_eq!(response.publisher_statement_version, Some(2));
            assert_eq!(response.publisher_release_sequence, Some(sequence.get()));
        }
    }

    #[tokio::test]
    async fn signed_directory_accepts_the_production_bzip2_profile() {
        let storage = HashMapStorage::default();
        let files = vec![
            ("a.txt".into(), Bytes::from_static(b"alpha")),
            ("nested/b.txt".into(), Bytes::from_static(b"beta")),
        ];
        let algorithm = ChunkAlgorithm::fixed_aligned(1024 * 1024, 100 * 1024);
        let prepared = storage
            .prepare_revision_from_files(
                "signed-directory".to_string(),
                "directory".to_string(),
                "logical".into(),
                0,
                None,
                files.clone(),
                algorithm,
            )
            .unwrap();
        let (sequence, envelope) = signed_release(&prepared, [23; 32], 1);
        let server = ProductionServer::new_ephemeral(storage)
            .with_publisher_policy(PublisherPolicy::required([envelope.public_key()]));
        let published = server
            .publish_release_item_from_files_as(
                "signed-directory".to_string(),
                "directory".to_string(),
                "logical".into(),
                None,
                files,
                algorithm,
                sequence,
                envelope,
            )
            .await
            .unwrap();
        assert_eq!(published.manifest.chunk_algorithm, algorithm);
        assert_eq!(published.manifest.entries.len(), 2);
        let (response, exact) = server
            .sync_manifest_response(ManifestRequest {
                artifact_id: "signed-directory".to_string(),
                version: Some(0),
            })
            .await
            .unwrap();
        assert_eq!(exact, published);
        assert_eq!(
            ChunkAlgorithm::try_from_proto_exact(response.chunk_algorithm.as_ref()).unwrap(),
            algorithm
        );
        assert_eq!(
            response.publisher_public_key,
            envelope.public_key().as_bytes()
        );
        assert_eq!(
            response.publisher_signature,
            envelope.signature().as_bytes()
        );
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
            .insert("exhausted".to_string(), unsigned(exhausted));
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
        behind
            .items
            .insert("stable-id".to_string(), unsigned(first));
        let behind_error = crate::metadata_state::validate_runtime_metadata(&behind).unwrap_err();
        assert!(matches!(
            behind_error,
            ServerError::State(message) if message.contains("highest historical revision")
        ));

        let mut mismatched_key = server.metadata.read().await.clone();
        let versions = mismatched_key.versions.get_mut("stable-id").unwrap();
        versions.remove(&second.manifest.version);
        versions.insert(second.manifest.version + 1, unsigned(second));
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
            .insert("stable-id".to_string(), unsigned(first.clone()));
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
        assert_eq!(
            versions.get(&first.manifest.version).unwrap().item(),
            &first
        );
        assert_eq!(
            versions.get(&second.manifest.version).unwrap().item(),
            &second
        );
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
        let storage = HashMapStorage::default();
        let bytes = Bytes::from(vec![7_u8; 8 * 1024 * 1024]);
        let prepared = storage
            .prepare_revision(
                "stable-id".to_string(),
                "display".to_string(),
                "artifact.bin".into(),
                0,
                None,
                bytes.clone(),
                ChunkAlgorithm::default(),
            )
            .unwrap();
        let (sequence, envelope) = signed_release(&prepared, [31; 32], 1);
        let policy = PublisherPolicy::required([envelope.public_key()]);
        let server = ProductionServer::new_ephemeral_with_metadata(storage, metadata)
            .with_publisher_policy(policy.clone())
            .with_metadata_store(store);
        let waiting_server = server.clone();
        let waiter = tokio::spawn(async move {
            waiting_server
                .publish_release_item_as(
                    "stable-id".to_string(),
                    "display".to_string(),
                    "artifact.bin".into(),
                    None,
                    bytes,
                    ChunkAlgorithm::default(),
                    sequence,
                    envelope,
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
                version: Some(published.item().manifest.version),
            })
            .await
            .unwrap();
        assert_eq!(&exact, published.item());
        let (reopened, durable) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |candidate| {
                crate::metadata_state::validate_runtime_metadata_with_policy(candidate, &policy)
            })
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
            let storage = HashMapStorage::default();
            let expected_bytes: &[u8] = if stage_index.is_multiple_of(2) {
                b"complete candidate"
            } else {
                b"directory candidate"
            };
            let algorithm = ChunkAlgorithm::default();
            let prepared = if stage_index.is_multiple_of(2) {
                storage
                    .prepare_revision(
                        "faulted".to_string(),
                        "display".to_string(),
                        "artifact.bin".into(),
                        0,
                        None,
                        Bytes::copy_from_slice(expected_bytes),
                        algorithm,
                    )
                    .unwrap()
            } else {
                storage
                    .prepare_revision_from_files(
                        "faulted".to_string(),
                        "display".to_string(),
                        "artifact".into(),
                        0,
                        None,
                        vec![(
                            "nested/file.bin".into(),
                            Bytes::copy_from_slice(expected_bytes),
                        )],
                        algorithm,
                    )
                    .unwrap()
            };
            let (sequence, envelope) =
                signed_release(&prepared, [u8::try_from(stage_index).unwrap() + 41; 32], 1);
            let policy = PublisherPolicy::required([envelope.public_key()]);
            let server = ProductionServer::new_ephemeral_with_metadata(storage, metadata.clone())
                .with_publisher_policy(policy.clone())
                .with_metadata_store(store);
            let result = if stage_index.is_multiple_of(2) {
                server
                    .publish_release_item_as(
                        "faulted".to_string(),
                        "display".to_string(),
                        "artifact.bin".into(),
                        None,
                        Bytes::copy_from_slice(expected_bytes),
                        algorithm,
                        sequence,
                        envelope,
                    )
                    .await
            } else {
                server
                    .publish_release_item_from_files_as(
                        "faulted".to_string(),
                        "display".to_string(),
                        "artifact".into(),
                        None,
                        vec![(
                            "nested/file.bin".into(),
                            Bytes::copy_from_slice(expected_bytes),
                        )],
                        algorithm,
                        sequence,
                        envelope,
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
                    |candidate| {
                        candidate.verify_storage(server.storage())?;
                        crate::metadata_state::validate_runtime_metadata_with_policy(
                            candidate, &policy,
                        )
                    },
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
                            .get_chunk_by_index(&item.item().metadata.root.hash, 0),
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
        assert_eq!(durable.items.get("directory").unwrap().item(), &published);
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
        assert_eq!(durable.items.get("single").unwrap().item(), &published);
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
        let storage = HashMapStorage::default();
        let algorithm = ChunkAlgorithm::default();
        let left_bytes = Bytes::from_static(b"left");
        let right_bytes = Bytes::from_static(b"right");
        let (left_sequence, left_envelope) = signed_release(
            &storage
                .prepare_revision(
                    "left".to_string(),
                    "left".to_string(),
                    "left.bin".into(),
                    0,
                    None,
                    left_bytes.clone(),
                    algorithm,
                )
                .unwrap(),
            [51; 32],
            1,
        );
        let (right_sequence, right_envelope) = signed_release(
            &storage
                .prepare_revision(
                    "right".to_string(),
                    "right".to_string(),
                    "right.bin".into(),
                    0,
                    None,
                    right_bytes.clone(),
                    algorithm,
                )
                .unwrap(),
            [53; 32],
            1,
        );
        let policy =
            PublisherPolicy::required([left_envelope.public_key(), right_envelope.public_key()]);
        let server = ProductionServer::new_ephemeral_with_metadata(storage, metadata)
            .with_publisher_policy(policy.clone())
            .with_metadata_store(store);
        let left_server = server.clone();
        let right_server = server.clone();
        let (left, right) = tokio::join!(
            left_server.publish_release_item_as(
                "left".to_string(),
                "left".to_string(),
                "left.bin".into(),
                None,
                left_bytes,
                algorithm,
                left_sequence,
                left_envelope,
            ),
            right_server.publish_release_item_as(
                "right".to_string(),
                "right".to_string(),
                "right.bin".into(),
                None,
                right_bytes,
                algorithm,
                right_sequence,
                right_envelope,
            )
        );
        let left = left.unwrap();
        let right = right.unwrap();
        let (_, durable) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |metadata| {
                metadata.verify_storage(server.storage())?;
                crate::metadata_state::validate_runtime_metadata_with_policy(metadata, &policy)
            })
            .unwrap();
        assert_eq!(durable.items.get("left").unwrap().item(), &left);
        assert_eq!(durable.items.get("right").unwrap().item(), &right);
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
        let storage = HashMapStorage::default();
        let algorithm = ChunkAlgorithm::default();
        let prepare = |artifact_id: &str, bytes: Bytes| {
            storage
                .prepare_revision(
                    artifact_id.to_string(),
                    artifact_id.to_string(),
                    "artifact.bin".into(),
                    0,
                    None,
                    bytes,
                    algorithm,
                )
                .unwrap()
        };
        let left_bytes = Bytes::from_static(b"left revision");
        let right_bytes = Bytes::from_static(b"right revision");
        let (left_sequence, left_envelope) =
            signed_release(&prepare("stable", left_bytes.clone()), [61; 32], 1);
        let (right_sequence, right_envelope) =
            signed_release(&prepare("stable", right_bytes.clone()), [63; 32], 2);
        let mut invalid_signature = *left_envelope.signature().as_bytes();
        invalid_signature[0] ^= 1;
        let invalid_envelope = PublisherReleaseEnvelope::from_parts(
            left_envelope.public_key(),
            PublisherSignature::from_bytes(invalid_signature),
        );
        let duplicate_bytes = Bytes::from_static(b"duplicate revision");
        let (duplicate_sequence, duplicate_envelope) =
            signed_release(&prepare("duplicate", duplicate_bytes.clone()), [61; 32], 1);
        let policy =
            PublisherPolicy::required([left_envelope.public_key(), right_envelope.public_key()]);
        let server = ProductionServer::new_ephemeral_with_metadata(storage, metadata)
            .with_publisher_policy(policy.clone())
            .with_metadata_store(store);
        let left_server = server.clone();
        let right_server = server.clone();
        let invalid_server = server.clone();
        let (left, right, invalid) = tokio::join!(
            left_server.publish_release_item_as(
                "stable".to_string(),
                "stable".to_string(),
                "artifact.bin".into(),
                None,
                left_bytes,
                algorithm,
                left_sequence,
                left_envelope,
            ),
            right_server.publish_release_item_as(
                "stable".to_string(),
                "stable".to_string(),
                "artifact.bin".into(),
                None,
                right_bytes,
                algorithm,
                right_sequence,
                right_envelope,
            ),
            invalid_server.publish_release_item_as(
                "stable".to_string(),
                "stable".to_string(),
                "artifact.bin".into(),
                None,
                Bytes::from_static(b"left revision"),
                algorithm,
                left_sequence,
                invalid_envelope,
            )
        );
        let left = left.unwrap();
        let right = right.unwrap();
        assert!(matches!(
            invalid.unwrap_err(),
            ServerError::Publication(PublicationError::InvalidSignature(_))
        ));
        let mut versions = [left.metadata.revision, right.metadata.revision];
        versions.sort_unstable();
        assert_eq!(versions, [0, 1]);

        let duplicate_left = server.clone();
        let duplicate_right = server.clone();
        let (first_duplicate, second_duplicate) = tokio::join!(
            duplicate_left.publish_release_item_as(
                "duplicate".to_string(),
                "duplicate".to_string(),
                "artifact.bin".into(),
                None,
                duplicate_bytes.clone(),
                algorithm,
                duplicate_sequence,
                duplicate_envelope,
            ),
            duplicate_right.publish_release_item_as(
                "duplicate".to_string(),
                "duplicate".to_string(),
                "artifact.bin".into(),
                None,
                duplicate_bytes,
                algorithm,
                duplicate_sequence,
                duplicate_envelope,
            )
        );
        assert_eq!(first_duplicate.unwrap(), second_duplicate.unwrap());

        let (_, durable) =
            crate::metadata_state::MetadataStateStore::open_with_validator(paths, |candidate| {
                candidate.verify_storage(server.storage())?;
                crate::metadata_state::validate_runtime_metadata_with_policy(candidate, &policy)
            })
            .unwrap();
        let history = durable.versions.get("stable").unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history.get(&0).unwrap().item().metadata.revision, 0);
        assert_eq!(history.get(&1).unwrap().item().metadata.revision, 1);
        assert_eq!(durable.versions.get("duplicate").unwrap().len(), 1);
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
