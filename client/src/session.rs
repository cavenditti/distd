//! Strict, isolated sessions for embedding the production client.
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use distd_core::{
    blob::BlobChunkSource,
    chunk_storage::{
        fs_storage::{FsStorage, FsStorageCacheConfig},
        node::ArcChunk,
    },
    chunks::ChunkInfo,
    item::Item,
    metadata::Item as ItemMetadata,
};

use crate::{
    client::Client, persistence::ClientState, server_url::CanonicalServerUrl, settings::Settings,
    ClientError,
};

/// A complete revision admitted by the configured publisher and rollback
/// policies, then verified and committed by the existing streaming client.
/// Fields are private: callers cannot attach authentication to an arbitrary Item.
#[derive(Clone, Debug)]
pub struct AuthenticatedRevision {
    item: Arc<Item>,
    expected: ItemMetadata,
    authority: CanonicalServerUrl,
    certificate_pin: [u8; 32],
    storage: Arc<FsStorage>,
}

impl AuthenticatedRevision {
    #[must_use]
    pub fn item(&self) -> &Item {
        &self.item
    }

    #[must_use]
    pub fn authority(&self) -> &CanonicalServerUrl {
        &self.authority
    }
}

impl BlobChunkSource for AuthenticatedRevision {
    fn read_chunk(&self, expected: &ChunkInfo) -> std::io::Result<ArcChunk> {
        self.storage.read_chunk(expected)
    }
}

/// Owns a production Client with private storage and no background fetch task.
/// Each download refreshes the catalog, then binds to that exact snapshot.
pub struct AuthenticatedSession {
    client: Client<FsStorage>,
    // Prevent separate embedded sessions/processes from racing the same disk
    // state or activation tree. OS locks release even on a crashed process.
    _state_lease: std::fs::File,
    _content_lease: std::fs::File,
}

impl AuthenticatedSession {
    /// Connect with an explicit TLS pin, enrollment token and required publisher
    /// allowlist. TOFU and unsigned compatibility are rejected before I/O.
    /// `settings.fsstorage.root` holds content; `state_directory` holds durable
    /// trust, rollback watermarks and storage checkpoints. Neither is global.
    pub async fn connect(
        settings: Settings,
        state_directory: PathBuf,
    ) -> Result<Self, ClientError> {
        if settings.server.allow_tofu
            || settings.server.certificate_fingerprint.is_none()
            || settings.server.publisher_policy.permits_unsigned()
            || settings
                .server
                .enrollment_token
                .as_deref()
                .is_none_or(|token| token.trim().is_empty())
        {
            return Err(ClientError::InsecureSession);
        }
        let root = PathBuf::from(&settings.fsstorage.root);
        let (storage, state, state_lease, content_lease) = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&state_directory)?;
            std::fs::create_dir_all(&root)?;
            let state_lease = exclusive_lease(&state_directory.join("session.lock"))?;
            let content_lease = exclusive_lease(&root.join(".distd-session.lock"))?;
            let state = ClientState::load_in(&state_directory)?;
            let storage = FsStorage::with_state_directory(
                root,
                FsStorageCacheConfig::default(),
                state_directory.join("storage"),
            )?;
            Ok::<_, ClientError>((storage, state, state_lease, content_lease))
        })
        .await??;
        Ok(Self {
            client: Client::new(storage, settings, state).await?,
            _state_lease: state_lease,
            _content_lease: content_lease,
        })
    }

    #[must_use]
    pub fn client_uuid(&self) -> uuid::Uuid {
        self.client.server.client_uuid()
    }

    /// Refresh metadata, authenticate and eagerly download one complete revision
    /// through the existing bounded QUIC → verified disk-pack → activation path.
    /// Unchanged cached chunks participate in normal possession negotiation.
    pub async fn download(
        &mut self,
        artifact_id: &str,
    ) -> Result<AuthenticatedRevision, ClientError> {
        self.client.server.fetch().await?;
        let expected = self
            .client
            .server
            .metadata_item(artifact_id)
            .await
            .ok_or_else(|| ClientError::FileNotFound(artifact_id.to_owned()))?;
        self.download_expected(expected).await
    }

    /// Fetch the exact version of a previously authenticated handle, never the
    /// current catalog version. All publisher/rollback checks run again: after
    /// accepting a newer release this can fail rollback protection, while reads
    /// from the already-resident old handle remain pinned to its original bytes.
    pub async fn redownload(
        &mut self,
        revision: &AuthenticatedRevision,
    ) -> Result<AuthenticatedRevision, ClientError> {
        if revision.authority != *self.client.server.canonical_url()
            || revision.certificate_pin != self.client.server.certificate_pin()
        {
            return Err(ClientError::RevisionAuthorityMismatch);
        }
        self.download_expected(revision.expected.clone()).await
    }

    async fn download_expected(
        &mut self,
        expected: ItemMetadata,
    ) -> Result<AuthenticatedRevision, ClientError> {
        // A local fixed namespace: remote paths and names never select it.
        let authority = self.client.server.canonical_url().clone();
        let destination = Path::new("artifacts")
            .join(
                blake3::hash(authority.as_str().as_bytes())
                    .to_hex()
                    .as_str(),
            )
            .join(
                blake3::hash(expected.artifact_id.as_bytes())
                    .to_hex()
                    .as_str(),
            );
        let item = self
            .client
            .get_expected(expected.artifact_id.clone(), expected.clone(), &destination)
            .await?;
        Ok(AuthenticatedRevision {
            item: Arc::new(item),
            expected,
            authority,
            certificate_pin: self.client.server.certificate_pin(),
            storage: self.client.storage.clone(),
        })
    }
}

fn exclusive_lease(path: &Path) -> std::io::Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    file.try_lock().map_err(|error| {
        std::io::Error::other(format!(
            "client session directory is in use or cannot be locked: {error}"
        ))
    })?;
    Ok(file)
}
