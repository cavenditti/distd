use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use tokio::time::sleep;

use crate::{
    error::{Client as ClientError, ServerRequest},
    server::Server,
    settings::Settings,
};

use std::{fs::File, io::Read};

use distd_core::{
    chunk_storage::{fs_storage::FsStorage, ChunkStorage, StoredChunkRef}, error::InvalidParameter, hash::Hash, item::Item, metadata::Item as ItemMetadata
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

    /// Associated server
    pub server: Server,

    /// Storage, implementing `ChunkStorage`
    pub storage: T,

    pub settings: Arc<Settings>,
}

impl<T> Client<T>
where
    T: ChunkStorage,
{
    pub async fn new(
        server_public_key: &[u8; 32],
        storage: T,
        settings: Settings,
    ) -> Result<Self, ClientError> {
        // Wait for server connection to get client uid
        let server = loop {
            match Server::new(
                &settings.server.url,
                &settings.client.name,
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
            return Err(ClientError::MissingItem);
        }

        Ok(Self {
            name: String::from(&settings.client.name),
            server,
            storage,
            settings: Arc::new(settings),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl Client<FsStorage> {
    pub async fn sync(self, target: &Path, path: &Path) -> Result<(), ClientError> {
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

        let from = self.storage.chunks(); // FIXME this could get very very large (optimize by only sending roots of sub-trees)
        tracing::trace!("Signalig to server we've got {from:?}");

        let root = self.transfer_diff(&item_metadata.root.hash, &from).await?;

        tracing::trace!("Reconstructed with {} bytes total", root.size());

        let new_item = self
            .storage
            .build_item(
                item_metadata.name.clone(),
                path,
                item_metadata.revision,
                item_metadata.description.clone(),
                root,
            )
            .ok_or(ClientError::ItemInsertion(
                "Cannot insert downloaded file".into(),
            ))?;

        tracing::debug!(
            "Updated {} to revision {}",
            new_item.metadata.path.to_string_lossy(),
            new_item.metadata.revision
        );
        Ok(())
    }
}

impl<T> Client<T>
where
    T: ChunkStorage + Clone + Send + 'static,
{
    async fn transfer_diff(&self, target: &Hash, from: &[Hash]) -> Result<Arc<StoredChunkRef>, ClientError> {
        let mut node_stream = self
            .server
            .transfer_diff(target, from)
            .await?;

        let mut n = None; // final node
        let mut i = 0; // node counter
        while let Some(node) = node_stream.message().await.map_err(ServerRequest::Grpc)? {
            let deser =
                bitcode::deserialize(&node.bitcode_hashtree).map_err(InvalidParameter::Bitcode)?;
            n = Some(
                self.storage
                    .try_fill_in(&deser)
                    .ok_or(ClientError::TreeReconstruct)?,
            );
            i += 1;
        }

        let n = n.ok_or(ClientError::TreeReconstruct)?;
        tracing::trace!("Reconstructed {i} nodes with {} bytes total", n.size());
        Ok(n)
    }

    async fn update(&mut self, new: &ItemMetadata) -> Result<Item, ClientError> {
        tracing::debug!(
            "Updating item '{}' @ {}",
            new.name,
            new.path.to_string_lossy()
        );

        let from = self.storage.chunks(); // FIXME this could get very very large
        let n = self.transfer_diff(&new.root.hash, &from).await?;

        let item = self
            .storage
            .build_item(
                new.name.clone(),
                new.path.clone(),
                new.revision,
                new.description.clone(),
                n,
            )
            .ok_or(ClientError::ItemInsertion(
                "Cannot insert downloaded file".into(),
            ))?;

        tracing::debug!("Got {item:?}");

        Ok(item)
    }

    /// Main client loop
    pub async fn client_loop(mut self) -> Result<(), ClientError> {
        tokio::spawn(self.server.clone().fetch_loop());

        loop {
            tokio::time::sleep(self.server.timeout).await;
            let items = self.server.metadata().await.items;
            for path in &self.settings.client.sync.clone() {
                tracing::debug!("Syncing '{}'", path.to_string_lossy());
                let i = items.get(path).ok_or(ClientError::Storage)?; //FIXME should fail on missing on server or sync other files anyway?
                self.update(i).await?;
            }
        }
    }
}

pub mod cli {
    use std::{env, path::PathBuf, str::FromStr};

    use distd_core::chunk_storage::fs_storage::FsStorage;

    use crate::client::Client;
    use crate::error::Client as ClientError;
    use crate::settings::Settings;

    pub async fn main() -> Result<(), ClientError> {
        tracing_subscriber::fmt()
            .with_target(false)
            .compact()
            .with_max_level(tracing::Level::INFO)
            .init();

        tracing::info!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));

        let cmd = std::env::args().nth(1).ok_or(ClientError::MissingCmd)?;
        let mut i = std::env::args();
        i.advance_by(2).map_err(|_| ClientError::MissingCmd)?; // FIXME may not be the right type
        let cmd_args = i.collect::<Vec<String>>();
        tracing::debug!("Running \"{cmd}\" {cmd_args:?}");

        let settings = Settings::new("ClientSettings")?;
        tracing::debug!("Settings: {settings:?}");

        let Ok(storage_root) = PathBuf::from_str(&settings.fsstorage.root);
        let storage = FsStorage::new(storage_root).map_err(|_| ClientError::Storage)?;
        let client = Client::new(&[0u8; 32], storage.clone(), settings).await?;

        match cmd.as_str() {
            "start" => client.client_loop().await,
            "sync" => sync(client, &cmd_args[..]).await, // TODO change name and use sync to explicitly request syncing of items subscripted to
            "publish" => todo!(),
            "subscribe" => todo!(),
            _ => {
                tracing::error!("Invalid command specified");
                Err(ClientError::InvalidCmd(cmd))
            }
        }
        .inspect_err(|e| tracing::error!("Fatal: {e}"))
    }

    async fn sync(client: Client<FsStorage>, args: &[String]) -> Result<(), ClientError> {
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

        client.sync(&target, &path).await
    }
}
