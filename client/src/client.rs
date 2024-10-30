use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use tokio::time::{sleep, Instant};
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::{
    error::Client as ClientError, persistence::ClientState, server::Server, settings::Settings,
};

use std::{fs::File, io::Read};

use distd_core::{
    chunk_storage::{fs_storage::FsStorage, node_stream::receiver, ChunkStorage},
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
    /// Client name
    name: String,

    //pub state: ClientState,
    /// Associated server
    pub server: Server,

    /// Storage, implementing `ChunkStorage`
    pub storage: T,

    pub settings: Arc<Settings>,

    pub state: Arc<ClientState>,
}

impl<T> Client<T>
where
    T: ChunkStorage,
{
    pub async fn new(
        server_public_key: &[u8; 32],
        storage: T,
        settings: Settings,
        mut state: ClientState,
    ) -> Result<Self, ClientError> {
        let client_uuid = state
            .persistent
            .client_uuid
            .as_ref()
            .and_then(|uuid_str| Uuid::from_str(&uuid_str).ok());

        // Wait for server connection to get client uid
        let server = loop {
            match Server::new(
                &settings.server.url,
                &settings.client.name,
                client_uuid,
                server_public_key,
            )
            .await
            {
                Ok(server) => break server,
                Err(e) => {
                    const T: u64 = 5;
                    tracing::warn!("Error: '{e}', retrying in {T} seconds");
                    sleep(Duration::from_secs(T)).await;
                }
            }
        };

        state.persistent.client_uuid = Some(server.client_uuid().to_string());
        state.persistent.commit().unwrap();

        // Check for missing paths on server
        let items = server.metadata().await.items;
        let server_paths: Vec<&PathBuf> = items.keys().collect();
        let missing: Vec<&PathBuf> = settings
            .client
            .sync
            .iter()
            .filter(|p| !server_paths.contains(p))
            .collect();
        if !missing.is_empty() {
            tracing::error!(
                "Some requested paths could not be found found in server: {}",
                missing
                    .iter()
                    .map(|p| format!("'{}'", p.to_string_lossy()))
                    .collect::<Vec<String>>()
                    .join(",")
            );
            // It probably shouldn't exit in this case
            //return Err(ClientError::MissingItem);
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
    pub async fn sync(&mut self, target: &Path, path: &Path) -> Result<Item, ClientError> {
        tracing::debug!("sync: {target:?} {path:?}");
        let mut buf = vec![];

        let path = self.storage.path(path);

        // If it exists read into buffer
        if path.exists() {
            tracing::trace!("File exists");
            File::open(&path)
                .and_then(|mut f| f.read_to_end(&mut buf))
                .map_err(ClientError::Io)?;
        }

        let server_metadata = self.server.metadata().await;
        let item_metadata = server_metadata
            .items
            .get(target)
            .ok_or(ClientError::FileNotFound(target.to_string_lossy().into()))?;

        let _item = self.storage.create_item(
            item_metadata.name.clone(),
            path.clone(),
            item_metadata.revision,
            item_metadata.description.clone(),
            buf.clone().into(),
        );

        self.update(item_metadata).await
    }
}

impl<T> Client<T>
where
    T: ChunkStorage + Send + 'static,
{
    /// Transfer a diff from the server
    ///
    /// Note: this function is item-agnostic, if using the FsStorage backend one should have already
    /// preallocated an item in order to be able to reconstruct sub-trees
    async fn transfer_diff(
        &mut self,
        target: ItemMetadata,
        request_version: Option<u32>,
        from_version: Option<u32>,
        from: &[Hash],
    ) -> Result<Item, ClientError> {
        let stream = self
            .server
            .transfer_diff(
                target.path.to_string_lossy().into_owned(),
                request_version,
                from_version,
                from,
            )
            .await?;

        let stream = stream.map(|x| x.unwrap().payload); // FIXME unwraps
        let stream = receiver(stream, 32, Duration::from_nanos(4800));

        self.storage
            .receive_item(
                target.name,
                target.path,
                target.revision,
                target.description,
                stream,
            )
            .await
            .map_err(ClientError::Core)
    }

    async fn update(&mut self, new_item_metadata: &ItemMetadata) -> Result<Item, ClientError> {
        tracing::info!(
            "Updating item '{}' at '{}'",
            new_item_metadata.name,
            new_item_metadata.path.to_string_lossy()
        );
        let now = Instant::now();

        let from = self.storage.chunks(); // FIXME this could get very very large

        let item = self
            .transfer_diff(
                // FIXME pass item versions
                new_item_metadata.clone(),
                None,
                None,
                &from,
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

        let mut latest: HashMap<PathBuf, Hash> = HashMap::default();

        loop {
            tokio::time::sleep(self.server.timeout).await;
            let items = self.server.metadata().await.items;
            for path in &self.settings.client.sync.clone() {
                if latest.get(path)
                    == self
                        .server
                        .metadata()
                        .await
                        .items
                        .get(path)
                        .map(|i| &i.root.hash)
                {
                    continue;
                }

                tracing::debug!("Syncing '{}'", path.to_string_lossy());
                let old_item = items.get(path).ok_or(ClientError::Storage)?; //FIXME should fail on missing on server or sync other files anyway?
                let item = self.update(old_item).await?;
                latest.insert(path.clone(), *item.root());
            }
        }
    }
}

pub mod cli {
    use std::{env, path::PathBuf, str::FromStr};

    use distd_core::chunk_storage::fs_storage::FsStorage;
    use distd_core::chunk_storage::hashmap_storage::HashMapStorage;

    use crate::client::Client;
    use crate::error::Client as ClientError;
    use crate::persistence::ClientState;
    use crate::settings::Settings;

    pub async fn main() -> Result<(), ClientError> {
        tracing_subscriber::fmt()
            .with_target(true)
            //.compact()
            .with_max_level(tracing::Level::DEBUG)
            .init();

        tracing::info!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

        let cmd = std::env::args().nth(1).ok_or(ClientError::MissingCmd)?;
        let mut i = std::env::args();
        i.advance_by(2).map_err(|_| ClientError::MissingCmd)?; // FIXME may not be the right type
        let cmd_args = i.collect::<Vec<String>>();
        tracing::debug!("Running \"{cmd}\" {cmd_args:?}");

        let settings = Settings::new("ClientSettings")?;
        tracing::debug!("Settings: {settings:?}");

        let state = ClientState::default();

        let Ok(storage_root) = PathBuf::from_str(&settings.fsstorage.root);
        let storage = FsStorage::new(storage_root);
        let storage = HashMapStorage::default(); // use this for benchmarking in order to avoid potential fs-related bottlenecks
        let client = Client::new(&[0u8; 32], storage, settings, state).await?;

        match cmd.as_str() {
            "start" => client.client_loop().await,
            //"sync" => sync(client, &cmd_args[..]).await, // TODO change name and use sync to explicitly request syncing of items subscripted to
            "publish" => todo!(),
            "subscribe" => todo!(),
            _ => {
                tracing::error!("Invalid command specified");
                Err(ClientError::InvalidCmd(cmd))
            }
        }
        .inspect_err(|e| tracing::error!("Fatal: {e}"))
    }

    async fn sync(mut client: Client<FsStorage>, args: &[String]) -> Result<(), ClientError> {
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

        client.sync(&target, &path).await.map(|_| ())
    }
}
