use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};

use tokio::time::sleep;

use crate::{
    error::{Client as ClientError, InvalidParameter, ServerRequest},
    server::Server,
    settings::Settings,
};

use std::{fmt::Write, fs::File, io::Read};

use http_body_util::BodyExt;
use hyper::body::Buf;
use hyper::http::uri::PathAndQuery;

use distd_core::{
    chunk_storage::{fs_storage::FsStorage, ChunkStorage},
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

    /// Associated server
    pub server: Server,

    /// Storage, implementing `ChunkStorage`
    pub storage: T,

    /// Items the client keeps updated
    pub items: Arc<HashMap<String, ItemMetadata>>,

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
        let url = settings.server.url.clone();
        let url = url.parse::<hyper::Uri>().map_err(InvalidParameter::Uri)?;

        // Only HTTP for now.
        if url.scheme_str() != Some("http") {
            return Err(ClientError::InvalidArgs(vec![url.to_string()]));
        }

        // Wait for server connection to get client uid
        let server = loop {
            match Server::new(url.clone(), &settings.client.name, server_public_key).await {
                Ok(server) => break server,
                Err(e) => {
                    const T: u64 = 5;
                    tracing::warn!("Error: '{e}', retrying in {T} seconds");
                    sleep(Duration::from_secs(T)).await;
                }
            }
        };

        Ok(Self {
            name: String::from(&settings.client.name),
            server,
            storage,
            items: Arc::default(),
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

        let from = self.storage.chunks(); // FIXME this could get very very large
        tracing::trace!("Signalig to server we've got {from:?}");

        let result = self
            .server
            .transfer_diff(&item_metadata.root.hash.to_string(), from)
            .await?;

        tracing::trace!("Got '{}' from server", result);

        let n = self
            .storage
            .try_fill_in(result)
            .ok_or(ClientError::TreeReconstruct)?;
        tracing::trace!("Reconstructed with {} bytes total", n.size());

        let new_item = self
            .storage
            .create_item(
                item_metadata.name.clone(),
                path,
                item_metadata.revision,
                item_metadata.description.clone(),
                n.clone_data().into(),
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
    /// Main client loop
    pub async fn client_loop(self) -> Result<(), ClientError> {
        self.server.fetch_loop().await;
        Ok(())
    }

    /// Fetch a resource from the server, mostly used for debug
    pub async fn rest(self, method: &str, url: PathAndQuery) -> Result<(), ClientError> {
        tracing::debug!("REST request {method} {url}");

        let mut response = self
            .server
            .send_request(method, url)
            .await
            .inspect(|r| tracing::debug!("Got {:?}", &r))?;

        let body = response
            .body_mut()
            .collect()
            .await
            .map_err(ServerRequest::Request)?
            .aggregate()
            .chunk()
            .to_vec();

        let body_str = String::from_utf8(body.clone()).unwrap_or(body.iter().fold(
            String::new(),
            |mut s, x| {
                let _ = write!(s, "{x:x?}");
                s
            },
        ));
        tracing::trace!("Body: `{body_str}`");
        Ok(())
    }
}

pub mod cli {
    use std::{env, path::PathBuf, str::FromStr};

    use hyper::http::uri::PathAndQuery;

    use distd_core::chunk_storage::fs_storage::FsStorage;

    use crate::client::Client;
    use crate::error::Client as ClientError;
    use crate::settings::Settings;

    pub async fn main() -> Result<(), ClientError> {
        tracing_subscriber::fmt()
            .with_target(false)
            .compact()
            .with_max_level(tracing::Level::TRACE)
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
            "loop" => client.client_loop().await,
            "rest" => rest(client, cmd_args).await,
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

    /// Fetch a resource from the server, mostly used for debug
    async fn rest(client: Client<FsStorage>, args: Vec<String>) -> Result<(), ClientError> {
        let (method, url) = args
            .first()
            .zip(args.get(1))
            .ok_or(ClientError::InvalidArgs(args.clone()))?;
        tracing::debug!("Fetch {method} {url}");

        // url should always start with exactly one "/"
        let url = format!("/{}", url.trim_start_matches('/'));
        let url = PathAndQuery::from_str(url.as_str())
            .map_err(|_| ClientError::InvalidArgs(args.clone()))?;

        client.rest(method, url).await
    }
}
