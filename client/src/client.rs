use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::{sync::mpsc, time::Instant};
use uuid::Uuid;

use crate::{
    error::Client as ClientError,
    persistence::{ClientState, DurableTrustReceipt, ServerTrustRecord},
    server::{LocalChunkBasis, ReceivedChunkBatch, Server},
    server_url::CanonicalServerUrl,
    settings::Settings,
};

use distd_core::{
    chunk_storage::{fs_storage::FsStorage, ChunkStorage},
    hash::Hash,
    item::Item,
    metadata::Item as ItemMetadata,
};

#[cfg(test)]
use distd_core::{chunk_storage::ReceivedChunk, chunks::ChunkInfo, item::Manifest};

#[derive(Debug)]
pub struct RegisterError;

#[derive(Debug, Clone)]
pub struct Client<T>
where
    T: ChunkStorage,
{
    name: String,
    pub server: Server,
    pub storage: Arc<T>,
    pub settings: Arc<Settings>,
    pub state: Arc<ClientState>,
}

impl<T> Client<T>
where
    T: ChunkStorage,
{
    pub async fn new(
        storage: T,
        settings: Settings,
        mut state: ClientState,
    ) -> Result<Self, ClientError> {
        let publisher_policy = settings.server.publisher_policy.clone();
        if publisher_policy.permits_unsigned() {
            tracing::warn!(
                "publisher verification is explicitly disabled; unsigned artifacts may be activated"
            );
        } else {
            tracing::info!(
                publisher_key_count = publisher_policy.key_count(),
                "publisher verification is required"
            );
        }
        let canonical_url = CanonicalServerUrl::parse(&settings.server.url)?;
        let configured_certificate = settings
            .server
            .certificate_fingerprint
            .as_deref()
            .map(blake3::Hash::from_hex)
            .transpose()
            .map_err(|_| ClientError::UnpinnedServer)?
            .map(|fingerprint| *fingerprint.as_bytes());
        let stored_record = state.persistent.record(&canonical_url).copied();
        let authority = enrollment_authority(
            stored_record,
            configured_certificate,
            settings.server.allow_tofu,
        )?;

        let pending = Server::begin_enrollment(
            canonical_url.clone(),
            &settings.client.name,
            authority.client_uuid,
            authority.certificate_pin,
            settings.server.enrollment_token.as_deref(),
            publisher_policy,
            state.rollback.clone(),
            settings.client.max_sync_memory_mib.bytes(),
        )
        .await?;

        let candidate = ServerTrustRecord {
            certificate_pin: pending.observed_pin(),
            client_uuid: pending.client_uuid(),
        };
        let ClientState {
            pid,
            persistent,
            rollback,
        } = state;
        let (persistent, server) =
            install_before_activation(persistent, canonical_url, candidate, |receipt| async move {
                pending.activate(receipt).await.map_err(ClientError::from)
            })
            .await?;
        state = ClientState {
            pid,
            persistent,
            rollback,
        };

        let mut missing = Vec::new();
        for path in &settings.client.sync {
            let artifact_id = path.to_string_lossy();
            if server.metadata_item(&artifact_id).await.is_none() {
                missing.push(path);
            }
        }
        if !missing.is_empty() {
            tracing::error!(
                "Some requested items could not be found on server: {}",
                missing
                    .iter()
                    .map(|p| format!("'{}'", p.to_string_lossy()))
                    .collect::<Vec<String>>()
                    .join(",")
            );
        }

        Ok(Self {
            name: String::from(&settings.client.name),
            server,
            storage: Arc::new(storage),
            settings: Arc::new(settings),
            state: Arc::new(state),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

async fn install_before_activation<T, Activate, ActivateFuture>(
    persistent: crate::persistence::ClientPersistentState,
    canonical_url: CanonicalServerUrl,
    candidate: ServerTrustRecord,
    activate: Activate,
) -> Result<(crate::persistence::ClientPersistentState, T), ClientError>
where
    Activate: FnOnce(DurableTrustReceipt) -> ActivateFuture,
    ActivateFuture: Future<Output = Result<T, ClientError>>,
{
    install_before_activation_with(
        persistent,
        canonical_url,
        candidate,
        |persistent, url, record| persistent.install(url, record),
        activate,
    )
    .await
}

async fn install_before_activation_with<T, Install, Activate, ActivateFuture>(
    mut persistent: crate::persistence::ClientPersistentState,
    canonical_url: CanonicalServerUrl,
    candidate: ServerTrustRecord,
    install: Install,
    activate: Activate,
) -> Result<(crate::persistence::ClientPersistentState, T), ClientError>
where
    Install: FnOnce(
            &mut crate::persistence::ClientPersistentState,
            CanonicalServerUrl,
            ServerTrustRecord,
        ) -> Result<DurableTrustReceipt, crate::persistence::PersistenceError>
        + Send
        + 'static,
    Activate: FnOnce(DurableTrustReceipt) -> ActivateFuture,
    ActivateFuture: Future<Output = Result<T, ClientError>>,
{
    let (persistent, receipt) = tokio::task::spawn_blocking(move || {
        let receipt = install(&mut persistent, canonical_url, candidate)?;
        Ok::<_, crate::persistence::PersistenceError>((persistent, receipt))
    })
    .await??;
    let active = activate(receipt).await?;
    Ok((persistent, active))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EnrollmentAuthority {
    client_uuid: Option<Uuid>,
    certificate_pin: Option<[u8; 32]>,
}

fn enrollment_authority(
    stored: Option<ServerTrustRecord>,
    configured_pin: Option<[u8; 32]>,
    allow_tofu: bool,
) -> Result<EnrollmentAuthority, ClientError> {
    match stored {
        Some(record) => {
            if configured_pin.is_some_and(|pin| pin != record.certificate_pin) {
                return Err(ClientError::TrustConflict);
            }
            Ok(EnrollmentAuthority {
                client_uuid: Some(record.client_uuid),
                certificate_pin: Some(record.certificate_pin),
            })
        }
        None if configured_pin.is_some() || allow_tofu => Ok(EnrollmentAuthority {
            client_uuid: None,
            certificate_pin: configured_pin,
        }),
        None => Err(ClientError::UnpinnedServer),
    }
}

impl Client<FsStorage> {
    async fn local_diff_basis(
        &self,
        path: PathBuf,
    ) -> Result<(Option<u32>, LocalChunkBasis), ClientError> {
        let storage = Arc::clone(&self.storage);
        tokio::task::spawn_blocking(move || {
            storage
                .item_for_path(&path)
                .map(|item| {
                    (
                        Some(item.metadata.revision),
                        LocalChunkBasis::new(item.chunks.iter().map(|chunk| chunk.hash).collect()),
                    )
                })
                .unwrap_or_default()
        })
        .await
        .map_err(|_| ClientError::StorageWorker)
    }

    async fn sync_transfer(
        &mut self,
        requested_artifact_id: String,
        target: ItemMetadata,
        destination: PathBuf,
        from: LocalChunkBasis,
    ) -> Result<Item, ClientError> {
        let activation = LocalActivationMetadata::from(&target);
        let t0 = Instant::now();
        let prepared = self
            .server
            .prepare_sync_artifact_expected(requested_artifact_id, target, from)
            .await?;
        let manifest_chunk_count = prepared.chunks().len();
        let local_basis_count = prepared.local_hashes().len();
        let manifest = prepared.manifest().clone();
        let chunk_infos = prepared.chunks().to_vec();
        let local_hashes = prepared.local_hashes().to_vec();
        let storage = Arc::clone(&self.storage);
        // One decoded batch may wait while one is written. Combined with the
        // transport batch limit this keeps handoff memory bounded without
        // forcing disk I/O onto the async network task.
        let (sender, mut receiver) = mpsc::channel::<ReceivedChunkBatch>(1);
        let ingest_worker = tokio::task::spawn_blocking(move || {
            let mut ingest = storage.begin_sync_ingest(
                activation.name,
                destination,
                activation.revision,
                activation.description,
                manifest,
                chunk_infos,
                local_hashes,
            )?;
            while let Some(batch) = receiver.blocking_recv() {
                for (index, chunk) in batch.into_chunks() {
                    ingest.push_chunk(index, &chunk)?;
                }
            }
            let received_chunk_count = ingest.received_chunks();
            let payload_bytes = ingest.received_bytes();
            let activation_started = Instant::now();
            let item = ingest.finish()?;
            Ok::<_, distd_core::error::Error>((
                item,
                received_chunk_count,
                payload_bytes,
                activation_started.elapsed(),
            ))
        });

        let transfer_result = prepared.transfer_into(sender).await;
        let network_elapsed = t0.elapsed();
        let storage_result = ingest_worker
            .await
            .map_err(|_| ClientError::StorageWorker)?;
        let wire_bytes = match transfer_result {
            Ok(wire_bytes) => wire_bytes,
            Err(error) => {
                if matches!(error, crate::error::ServerRequest::StorageIngestClosed) {
                    if let Err(storage_error) = storage_result {
                        return Err(ClientError::Core(storage_error));
                    }
                }
                return Err(ClientError::ServerRequest(error));
            }
        };
        let (item, received_chunk_count, payload_bytes, store_elapsed) =
            storage_result.map_err(ClientError::Core)?;

        tracing::info!(
            "distd_sync_local_basis_chunks={} distd_sync_manifest_chunks={} distd_sync_wire_bytes={wire_bytes} distd_sync_payload_bytes={payload_bytes} distd_sync_received_chunks={} distd_network_ms={} distd_store_ms={}",
            local_basis_count,
            manifest_chunk_count,
            received_chunk_count,
            network_elapsed.as_millis(),
            store_elapsed.as_millis(),
        );

        Ok(item)
    }

    pub async fn get(&mut self, target: &Path, path: &Path) -> Result<Item, ClientError> {
        tracing::debug!("sync: {target:?} {path:?}");

        let resolved_path = self.storage.path(path).map_err(ClientError::Core)?;
        let (_, from) = self.local_diff_basis(resolved_path).await?;

        // Look up by artifact_id (target path string used as artifact_id)
        let target_str = target.to_string_lossy().to_string();
        let item_metadata = self
            .server
            .metadata_item(&target_str)
            .await
            .ok_or(ClientError::FileNotFound(target_str.clone()))?;

        tracing::info!(
            "Fetching item '{}' at '{}' (local chunks: {})",
            item_metadata.name,
            item_metadata.path.to_string_lossy(),
            from.len()
        );
        let now = Instant::now();

        let item = self
            .sync_transfer(target_str, item_metadata, path.to_path_buf(), from)
            .await?;

        tracing::info!(
            "Got {} v{}, {} bytes after {:.4}s",
            item.metadata.name,
            item.metadata.revision,
            item.size(),
            now.elapsed().as_secs_f32()
        );

        Ok(item)
    }

    async fn update(
        &mut self,
        requested_artifact_id: &str,
        new_item_metadata: &ItemMetadata,
        destination: &Path,
    ) -> Result<Item, ClientError> {
        tracing::info!(
            "Updating item '{}' at '{}'",
            new_item_metadata.name,
            new_item_metadata.path.to_string_lossy()
        );
        let now = Instant::now();

        let local_path = self.storage.path(destination).map_err(ClientError::Core)?;
        let (from_version, from) = self.local_diff_basis(local_path).await?;

        tracing::info!(
            "Requesting revision {} from local revision {:?} with {} known chunks",
            new_item_metadata.revision,
            from_version,
            from.len()
        );

        let item = self
            .sync_transfer(
                requested_artifact_id.to_owned(),
                new_item_metadata.clone(),
                destination.to_path_buf(),
                from,
            )
            .await?;

        tracing::info!(
            "Got {} v{}, {} bytes after {:.4}s",
            item.metadata.name,
            item.metadata.revision,
            item.size(),
            now.elapsed().as_secs_f32()
        );

        Ok(item)
    }

    /// Main client loop
    pub async fn client_loop(mut self) -> Result<(), ClientError> {
        tokio::spawn(self.server.clone().fetch_loop());

        let mut latest: HashMap<String, Hash> = HashMap::default();

        loop {
            tokio::time::sleep(self.server.timeout).await;
            for path in &self.settings.client.sync.clone() {
                let artifact_id = path.to_string_lossy().to_string();
                let item = self.server.metadata_item(&artifact_id).await;
                if latest.get(&artifact_id) == item.as_ref().map(|item| &item.root.hash) {
                    continue;
                }

                tracing::debug!("Syncing '{artifact_id}'");
                let old_item = item.ok_or(ClientError::Storage)?;
                let item = self.update(&artifact_id, &old_item, path).await?;
                latest.insert(artifact_id, *item.root());
            }
        }
    }
}

struct LocalActivationMetadata {
    name: String,
    revision: u32,
    description: Option<String>,
}

impl From<&ItemMetadata> for LocalActivationMetadata {
    fn from(remote: &ItemMetadata) -> Self {
        Self {
            name: remote.name.clone(),
            revision: remote.revision,
            description: remote.description.clone(),
        }
    }
}

#[cfg(test)]
async fn commit_sync_storage(
    storage: Arc<FsStorage>,
    activation: LocalActivationMetadata,
    destination: PathBuf,
    manifest: Manifest,
    chunk_infos: Vec<ChunkInfo>,
    from: Vec<Hash>,
    received_chunks: Vec<ReceivedChunk>,
) -> Result<Item, ClientError> {
    tokio::task::spawn_blocking(move || {
        storage.receive_sync_item_shared(
            activation.name,
            destination,
            activation.revision,
            activation.description,
            &manifest,
            &chunk_infos,
            &from,
            received_chunks,
        )
    })
    .await?
    .map_err(ClientError::from)
}

pub mod cli {
    use std::{env, path::PathBuf, str::FromStr};

    use distd_core::chunk_storage::fs_storage::FsStorage;

    use crate::client::Client;
    use crate::error::Client as ClientError;
    use crate::persistence::ClientState;
    use crate::settings::Settings;

    pub async fn main() -> Result<(), ClientError> {
        let settings = Settings::new("ClientSettings")?;

        tracing_subscriber::fmt()
            .with_target(true)
            .with_writer(std::io::stderr)
            //.compact()
            .with_max_level(tracing::Level::from_str(&settings.log.level).unwrap())
            .init();

        tracing::info!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        tracing::debug!("Settings: {settings:?}");

        let cmd = std::env::args().nth(1).ok_or(ClientError::MissingCmd)?;
        let cmd_args = std::env::args().skip(2).collect::<Vec<String>>();

        tracing::debug!("Running \"{cmd}\" {cmd_args:?}");

        let state = tokio::task::spawn_blocking(ClientState::load).await??;
        tracing::trace!("Client state initialized");

        let Ok(storage_root) = PathBuf::from_str(&settings.fsstorage.root);
        tracing::trace!("Storage root: {}", storage_root.to_string_lossy());
        let storage = tokio::task::spawn_blocking(move || FsStorage::new(storage_root)).await??;
        //let storage = HashMapStorage::default(); // use this for benchmarking in order to avoid potential fs-related bottlenecks
        tracing::trace!("Client storage initialized");
        let client = Client::new(storage, settings, state).await?;
        tracing::trace!("Client initialized");

        match cmd.as_str() {
            "start" => client.client_loop().await,
            "get" => get(client, &cmd_args[..]).await,
            // TODO add "sync: to explicitly request syncing of items subscripted to?
            "publish" => todo!(),
            "subscribe" => todo!(),
            _ => {
                tracing::error!("Invalid command specified");
                Err(ClientError::InvalidCmd(cmd))
            }
        }
        .inspect_err(|e| tracing::error!("Fatal: {e}"))
    }

    async fn get(mut client: Client<FsStorage>, args: &[String]) -> Result<(), ClientError> {
        let first = args
            .first()
            .ok_or(ClientError::InvalidArgs(args.to_owned()))?;

        let (target, path) = match args.len() {
            1 => (first, first),
            2 => (
                first,
                args.get(1)
                    .ok_or(ClientError::InvalidArgs(args.to_owned()))?,
            ),
            _ => return Err(ClientError::InvalidArgs(args.to_owned())),
        };

        let Ok(path) = PathBuf::from_str(path);
        let Ok(target) = PathBuf::from_str(target.as_str());

        client.get(&target, &path).await.map(|_| ())
    }
}

#[cfg(test)]
mod trust_tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use uuid::Uuid;

    use super::{
        enrollment_authority, install_before_activation, install_before_activation_with,
        EnrollmentAuthority,
    };
    use crate::{
        error::Client as ClientError,
        persistence::{ClientPersistentState, ServerTrustRecord},
        server_url::CanonicalServerUrl,
    };

    #[test]
    fn enrollment_authority_selects_pin_and_uuid_from_one_record() {
        let uuid = Uuid::from_u128(7);
        let pin = [0x5a; 32];
        assert_eq!(
            enrollment_authority(
                Some(ServerTrustRecord {
                    certificate_pin: pin,
                    client_uuid: uuid,
                }),
                Some(pin),
                false,
            )
            .unwrap(),
            EnrollmentAuthority {
                client_uuid: Some(uuid),
                certificate_pin: Some(pin),
            }
        );
    }

    #[test]
    fn enrollment_authority_rejects_pin_conflict_before_connection() {
        let result = enrollment_authority(
            Some(ServerTrustRecord {
                certificate_pin: [1; 32],
                client_uuid: Uuid::from_u128(9),
            }),
            Some([2; 32]),
            true,
        );
        assert!(matches!(result, Err(ClientError::TrustConflict)));
    }

    #[test]
    fn enrollment_authority_requires_pin_or_explicit_tofu_for_new_server() {
        assert!(matches!(
            enrollment_authority(None, None, false),
            Err(ClientError::UnpinnedServer)
        ));
        assert_eq!(
            enrollment_authority(None, None, true).unwrap(),
            EnrollmentAuthority {
                client_uuid: None,
                certificate_pin: None,
            }
        );
        assert_eq!(
            enrollment_authority(None, Some([3; 32]), false).unwrap(),
            EnrollmentAuthority {
                client_uuid: None,
                certificate_pin: Some([3; 32]),
            }
        );
    }

    #[tokio::test]
    async fn durable_install_completes_before_activation() {
        let temporary = tempfile::tempdir().unwrap();
        let current = temporary.path().join("trust.state");
        let legacy = temporary.path().join("legacy.json");
        let persistent =
            ClientPersistentState::load_from_paths(current.clone(), legacy.clone()).unwrap();
        let url = CanonicalServerUrl::parse("quic://example.com:443").unwrap();
        let record = ServerTrustRecord {
            certificate_pin: [0xa5; 32],
            client_uuid: Uuid::from_u128(11),
        };
        let inspect_url = url.clone();
        let (_, activated) =
            install_before_activation(persistent, url, record, |_receipt| async move {
                let reloaded = ClientPersistentState::load_from_paths(current, legacy).unwrap();
                assert_eq!(reloaded.record(&inspect_url), Some(&record));
                Ok::<_, ClientError>(true)
            })
            .await
            .unwrap();
        assert!(activated);
    }

    #[tokio::test]
    async fn failed_install_never_invokes_activation() {
        let temporary = tempfile::tempdir().unwrap();
        let absent_parent = temporary.path().join("absent");
        let persistent = ClientPersistentState::load_from_paths(
            absent_parent.join("trust.state"),
            absent_parent.join("legacy.json"),
        )
        .unwrap();
        let activated = Arc::new(AtomicBool::new(false));
        let activation_flag = Arc::clone(&activated);
        let result = install_before_activation(
            persistent,
            CanonicalServerUrl::parse("quic://example.com:443").unwrap(),
            ServerTrustRecord {
                certificate_pin: [7; 32],
                client_uuid: Uuid::from_u128(12),
            },
            |_receipt| async move {
                activation_flag.store(true, Ordering::SeqCst);
                Ok::<_, ClientError>(())
            },
        )
        .await;
        assert!(matches!(result, Err(ClientError::Persistence(_))));
        assert!(!activated.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn indeterminate_commit_never_invokes_activation() {
        let temporary = tempfile::tempdir().unwrap();
        let state_path = temporary.path().join("trust.state");
        let persistent = ClientPersistentState::load_from_paths(
            state_path.clone(),
            temporary.path().join("legacy.json"),
        )
        .unwrap();
        let error_path = state_path;
        let activated = Arc::new(AtomicBool::new(false));
        let activation_flag = Arc::clone(&activated);
        let result = install_before_activation_with(
            persistent,
            CanonicalServerUrl::parse("quic://example.com:443").unwrap(),
            ServerTrustRecord {
                certificate_pin: [8; 32],
                client_uuid: Uuid::from_u128(13),
            },
            move |persistent, url, record| {
                let _published = persistent.install(url, record)?;
                Err(crate::persistence::PersistenceError::Durable(
                    distd_core::durable_state::DurableStateError::CommitIndeterminate {
                        path: error_path,
                        source: std::io::Error::other("injected indeterminate commit"),
                    },
                ))
            },
            |_receipt| async move {
                activation_flag.store(true, Ordering::SeqCst);
                Ok::<_, ClientError>(())
            },
        )
        .await;
        assert!(matches!(result, Err(ClientError::Persistence(_))));
        assert!(!activated.load(Ordering::SeqCst));
    }

    #[test]
    fn separate_authorities_never_share_client_identity() {
        let temporary = tempfile::tempdir().unwrap();
        let mut persistent = ClientPersistentState::load_from_paths(
            temporary.path().join("trust.state"),
            temporary.path().join("legacy.json"),
        )
        .unwrap();
        let server_a = CanonicalServerUrl::parse("quic://a.example:443").unwrap();
        let server_b = CanonicalServerUrl::parse("quic://b.example:443").unwrap();
        let record_a = ServerTrustRecord {
            certificate_pin: [0xa; 32],
            client_uuid: Uuid::from_u128(101),
        };
        let record_b = ServerTrustRecord {
            certificate_pin: [0xb; 32],
            client_uuid: Uuid::from_u128(202),
        };
        persistent.install(server_a.clone(), record_a).unwrap();
        persistent.install(server_b.clone(), record_b).unwrap();

        let authority_a =
            enrollment_authority(persistent.record(&server_a).copied(), None, false).unwrap();
        let authority_b =
            enrollment_authority(persistent.record(&server_b).copied(), None, false).unwrap();
        assert_eq!(authority_a.client_uuid, Some(record_a.client_uuid));
        assert_eq!(authority_a.certificate_pin, Some(record_a.certificate_pin));
        assert_eq!(authority_b.client_uuid, Some(record_b.client_uuid));
        assert_eq!(authority_b.certificate_pin, Some(record_b.certificate_pin));
        assert_ne!(authority_a.client_uuid, authority_b.client_uuid);
    }
}

#[cfg(test)]
mod chunk_cache_tests {
    use std::{
        io::Read,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use distd_core::{
        chunk_storage::{
            fs_storage::{FsStorage, FsStorageCacheConfig},
            hashmap_storage::HashMapStorage,
            ChunkStorage, ReceivedChunk,
        },
        chunks::{ChunkAlgorithm, CHUNK_SIZE},
        hash::Hash,
        item::Item,
    };

    use super::{commit_sync_storage, LocalActivationMetadata};

    fn prepared_revision(
        source: &HashMapStorage,
        destination: &Path,
        revision: u32,
        bytes: &[u8],
    ) -> Item {
        let root = source.insert(bytes.to_vec().into()).unwrap();
        Item::new(
            "cache-ceiling".to_string(),
            destination.to_path_buf(),
            revision,
            None,
            &root,
            ChunkAlgorithm::Fixed256K,
        )
        .with_artifact_id("cache-ceiling".to_string())
    }

    fn assert_empty_bounded_cache(storage: &FsStorage) {
        let stats = storage.chunk_cache_stats();
        assert_eq!(stats.max_bytes, 1);
        assert_eq!(stats.current_bytes, 0);
        assert_eq!(stats.peak_bytes, 0);
        assert_eq!(stats.retained_entries, 0);
        assert_eq!(stats.recency_entries, 0);
        assert_eq!(stats.shared_backings, 0);
        assert_eq!(stats.shared_cached_views, 0);
        assert!(stats.invariants_hold);
    }

    fn read_all(mut file: std::fs::File) -> Vec<u8> {
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        bytes
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn production_sync_commit_respects_one_byte_cache_with_shared_bulk_views() {
        let temporary = tempfile::tempdir().unwrap();
        let destination = temporary.path().join("cache-ceiling.bin");
        let storage = Arc::new(
            FsStorage::with_cache_config(
                temporary.path().to_path_buf(),
                FsStorageCacheConfig {
                    max_chunk_bytes: 1,
                    max_tree_entries: 1,
                },
            )
            .unwrap(),
        );
        let source = HashMapStorage::default();

        let initial_bytes = (0..CHUNK_SIZE * 2)
            .map(|index| ((index * 17) % 251) as u8)
            .collect::<Vec<_>>();
        let initial = prepared_revision(&source, &destination, 1, &initial_bytes);
        let initial_bulk = ReceivedChunk::from_vec(initial_bytes.clone());
        let held_initial_bulk = initial_bulk.clone();
        let initial_views = vec![
            initial_bulk.slice(0..CHUNK_SIZE),
            initial_bulk.slice(CHUNK_SIZE..initial_bytes.len()),
        ];
        let first = commit_sync_storage(
            Arc::clone(&storage),
            LocalActivationMetadata::from(&initial.metadata),
            destination.clone(),
            initial.manifest.clone(),
            initial.chunks.clone(),
            Vec::new(),
            initial_views,
        )
        .await
        .unwrap();
        assert_eq!(first.metadata.root, initial.metadata.root);
        assert_eq!(held_initial_bulk.as_ref(), initial_bytes);
        assert_empty_bounded_cache(&storage);

        let mut changed_bytes = initial_bytes.clone();
        for (offset, byte) in changed_bytes[CHUNK_SIZE..].iter_mut().enumerate() {
            *byte = byte.wrapping_add(29) ^ (offset as u8);
        }
        let changed = prepared_revision(&source, &destination, 2, &changed_bytes);
        let changed_bulk = ReceivedChunk::copy_from_slice(&changed_bytes[CHUNK_SIZE..]);
        let held_changed_bulk = changed_bulk.clone();
        let local_hashes = first
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect::<Vec<Hash>>();
        let second = commit_sync_storage(
            Arc::clone(&storage),
            LocalActivationMetadata::from(&changed.metadata),
            destination.clone(),
            changed.manifest.clone(),
            changed.chunks.clone(),
            local_hashes,
            vec![changed_bulk],
        )
        .await
        .unwrap();
        assert_eq!(second.metadata.root, changed.metadata.root);
        assert_eq!(held_changed_bulk.as_ref(), &changed_bytes[CHUNK_SIZE..]);
        assert_empty_bounded_cache(&storage);

        let active = storage
            .open_active_revision(&destination)
            .unwrap()
            .expect("changed revision is active");
        assert_eq!(read_all(active.open_file().unwrap()), changed_bytes);
        drop(active);
        drop(storage);

        let reopened = FsStorage::with_cache_config(
            temporary.path().to_path_buf(),
            FsStorageCacheConfig {
                max_chunk_bytes: 1,
                max_tree_entries: 1,
            },
        )
        .unwrap();
        let active = reopened
            .open_active_revision(&destination)
            .unwrap()
            .expect("changed revision survives restart");
        assert_eq!(active.item().metadata.root, changed.metadata.root);
        assert_eq!(read_all(active.open_file().unwrap()), changed_bytes);
        assert_empty_bounded_cache(&reopened);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remote_deployment_hint_cannot_select_activation_destination() {
        let temporary = tempfile::tempdir().unwrap();
        let storage = Arc::new(FsStorage::new(temporary.path().to_path_buf()).unwrap());
        let source = HashMapStorage::default();
        let payload = vec![41u8; CHUNK_SIZE];
        let mut remote = prepared_revision(&source, Path::new("server-hint.bin"), 1, &payload);
        let escaped = temporary
            .path()
            .parent()
            .unwrap()
            .join(format!("distd-remote-destination-{}", uuid::Uuid::new_v4()));
        remote.metadata.path = escaped.clone();

        let item = commit_sync_storage(
            Arc::clone(&storage),
            LocalActivationMetadata::from(&remote.metadata),
            PathBuf::from("managed.bin"),
            remote.manifest.clone(),
            remote.chunks.clone(),
            Vec::new(),
            vec![ReceivedChunk::from_vec(payload.clone())],
        )
        .await
        .unwrap();

        assert_eq!(item.metadata.path, PathBuf::from("managed.bin"));
        assert_eq!(
            std::fs::read(temporary.path().join("managed.bin")).unwrap(),
            payload
        );
        assert!(!escaped.exists());
    }
}
