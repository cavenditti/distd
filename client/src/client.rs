use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use tokio::time::Instant;
use uuid::Uuid;

use crate::{
    error::Client as ClientError, persistence::ClientState, server::Server, settings::Settings,
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
    pub storage: T,
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
        let client_uuid = state
            .persistent
            .client_uuid
            .as_ref()
            .and_then(|uuid_str| Uuid::from_str(uuid_str).ok());
        let configured_certificate = settings
            .server
            .certificate_fingerprint
            .as_deref()
            .map(blake3::Hash::from_hex)
            .transpose()
            .map_err(|_| ClientError::UnpinnedServer)?
            .map(|fingerprint| *fingerprint.as_bytes());
        let persisted_certificate = state
            .persistent
            .server_certificates
            .get(&settings.server.url)
            .and_then(|fingerprint| blake3::Hash::from_hex(fingerprint).ok())
            .map(|fingerprint| *fingerprint.as_bytes());
        let expected_certificate = configured_certificate.or(persisted_certificate);
        if expected_certificate.is_none() && !settings.server.allow_tofu {
            return Err(ClientError::UnpinnedServer);
        }

        let server = Server::new(
            &settings.server.url,
            &settings.client.name,
            client_uuid,
            expected_certificate,
            settings.server.enrollment_token.as_deref(),
        )
        .await?;

        state.persistent.client_uuid = Some(server.client_uuid().to_string());
        state.persistent.server_certificates.insert(
            settings.server.url.clone(),
            blake3::Hash::from_bytes(server.pub_key)
                .to_hex()
                .to_string(),
        );
        state.persistent.commit().unwrap();

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
            storage,
            settings: Arc::new(settings),
            state: Arc::new(state),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
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
        let payload_bytes = received_chunks
            .iter()
            .map(|chunk| chunk.len() as u64)
            .sum::<u64>();

        let t1 = Instant::now();
        let item = self
            .storage
            .receive_sync_item_shared(
                target.name,
                destination,
                target.revision,
                target.description,
                &manifest,
                &chunk_infos,
                from,
                received_chunks,
            )
            .map_err(ClientError::Core)?;
        let store_elapsed = t1.elapsed();

        tracing::info!(
            "distd_sync_local_basis_chunks={} distd_sync_manifest_chunks={} distd_sync_wire_bytes={wire_bytes} distd_sync_payload_bytes={payload_bytes} distd_sync_received_chunks={} distd_network_ms={} distd_store_ms={}",
            from.len(),
            chunk_infos.len(),
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

        let state = ClientState::default();
        tracing::trace!("Client state initialized");

        let Ok(storage_root) = PathBuf::from_str(&settings.fsstorage.root);
        tracing::trace!("Storage root: {}", storage_root.to_string_lossy());
        let storage = FsStorage::new(storage_root);
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
