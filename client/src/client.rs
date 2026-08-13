use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
};

use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    error::Client as ClientError,
    persistence::{ClientState, DurableTrustReceipt, ServerTrustRecord},
    server::Server,
    server_url::CanonicalServerUrl,
    settings::Settings,
};

use distd_core::{
    chunk_storage::{fs_storage::FsStorage, ChunkStorage},
    hash::Hash,
    item::Item,
    metadata::Item as ItemMetadata,
};

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
        )
        .await?;

        let candidate = ServerTrustRecord {
            certificate_pin: pending.observed_pin(),
            client_uuid: pending.client_uuid(),
        };
        let ClientState { pid, persistent } = state;
        let (persistent, server) =
            install_before_activation(persistent, canonical_url, candidate, |receipt| async move {
                pending.activate(receipt).await.map_err(ClientError::from)
            })
            .await?;
        state = ClientState { pid, persistent };

        let items = server.metadata().await.items;
        let server_ids: Vec<&String> = items.keys().collect();
        let missing: Vec<&PathBuf> = settings
            .client
            .sync
            .iter()
            .filter(|p| {
                let aid = p.to_string_lossy().to_string();
                !server_ids.iter().any(|s| **s == aid)
            })
            .collect();
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
    fn local_diff_basis(&self, path: &Path) -> (Option<u32>, Vec<Hash>) {
        self.storage
            .item_for_path(path)
            .map(|item| {
                (
                    Some(item.metadata.revision),
                    item.chunks.iter().map(|chunk| chunk.hash).collect(),
                )
            })
            .unwrap_or_default()
    }

    async fn sync_transfer(
        &mut self,
        target: ItemMetadata,
        destination: PathBuf,
        from: &[Hash],
    ) -> Result<Item, ClientError> {
        let t0 = Instant::now();
        let (manifest, chunk_infos, received_chunks, wire_bytes) = self
            .server
            .sync_artifact_shared(&target.artifact_id, from)
            .await?;
        let network_elapsed = t0.elapsed();

        let received_chunk_count = received_chunks.len();
        let manifest_chunk_count = chunk_infos.len();
        let payload_bytes = received_chunks
            .iter()
            .map(|chunk| chunk.len() as u64)
            .sum::<u64>();

        let t1 = Instant::now();
        let storage = Arc::clone(&self.storage);
        let local_basis_count = from.len();
        let from = from.to_vec();
        let item = tokio::task::spawn_blocking(move || {
            storage.receive_sync_item_shared(
                target.name,
                destination,
                target.revision,
                target.description,
                &manifest,
                &chunk_infos,
                &from,
                received_chunks,
            )
        })
        .await??;
        let store_elapsed = t1.elapsed();

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
        let (_, from) = self.local_diff_basis(&resolved_path);

        let server_metadata = self.server.metadata().await;
        // Look up by artifact_id (target path string used as artifact_id)
        let target_str = target.to_string_lossy().to_string();
        let item_metadata = server_metadata
            .items
            .get(&target_str)
            .ok_or(ClientError::FileNotFound(target_str.clone()))?;

        tracing::info!(
            "Fetching item '{}' at '{}' (local chunks: {})",
            item_metadata.name,
            item_metadata.path.to_string_lossy(),
            from.len()
        );
        let now = Instant::now();

        let item = self
            .sync_transfer(item_metadata.clone(), path.to_path_buf(), &from)
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
        let (from_version, from) = self.local_diff_basis(&local_path);

        tracing::info!(
            "Requesting revision {} from local revision {:?} with {} known chunks",
            new_item_metadata.revision,
            from_version,
            from.len()
        );

        let item = self
            .sync_transfer(new_item_metadata.clone(), destination.to_path_buf(), &from)
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
            let items = self.server.metadata().await.items;
            for path in &self.settings.client.sync.clone() {
                let artifact_id = path.to_string_lossy().to_string();
                if latest.get(&artifact_id) == items.get(&artifact_id).map(|i| &i.root.hash) {
                    continue;
                }

                tracing::debug!("Syncing '{artifact_id}'");
                let old_item = items.get(&artifact_id).ok_or(ClientError::Storage)?;
                let item = self.update(old_item, path).await?;
                latest.insert(artifact_id, *item.root());
            }
        }
    }
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
