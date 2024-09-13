//use std::{net::SocketAddr
use crate::error::{InvalidParameter, ServerRequest};

use blake3::Hash;
use std::{fmt::Debug, io::Read, str::FromStr, sync::Arc, time::Duration};
use uuid::Uuid;

use http_body_util::{BodyExt, Empty};
use hyper::{
    body::{Buf, Bytes, Incoming},
    client::conn::http1::SendRequest,
    http::request::Builder,
    http::uri::PathAndQuery,
    Request, Response,
};
use tokio::{sync::RwLock, time::Instant};

//use ring::agreement::PublicKey;

use distd_core::{chunks::OwnedHashTreeNode, metadata::Server as ServerMetadata, version::VERSION};

use crate::connection;

/// Shared server-related data to be kept behind an async lock
#[derive(Debug)]
struct SharedServer {
    /// global server metadata
    pub metadata: ServerMetadata,

    /// last time metadata was fetched from server
    pub last_update: Instant,

    /// sender for REST requests to server
    pub sender: SendRequest<Empty<Bytes>>,
}

/// Server representation used by clients
#[derive(Debug, Clone)]
pub struct Server {
    //pub connection: ..

    // server address
    //pub addr: SocketAddr,
    /// server url
    pub url: hyper::Uri,

    /// server Ed25519 public key
    pub pub_key: [u8; 32], // TODO Check this

    /// Client Uuid assigned to client from server
    client_uid: Option<Uuid>,

    /// Shared data
    shared: Arc<RwLock<SharedServer>>,

    /// Elapsed time between server fetches
    timeout: Duration,
}

impl Server {
    fn request_builder(url: &hyper::Uri, method: &str) -> Builder {
        Request::builder()
            .uri(url.clone())
            .method(method)
            .header(hyper::header::HOST, url.authority().unwrap().as_str())
    }

    /// Send a request to the server
    async fn send_request_raw(
        url: hyper::Uri,
        sender: &mut SendRequest<Empty<Bytes>>,
        method: &str,
    ) -> Result<Response<Incoming>, ServerRequest> {
        // Prepare request
        let request = Self::request_builder(&url, method).body(Empty::<Bytes>::new())?;

        // Fetch from url
        let res = sender.send_request(request).await?;

        Ok(res)
    }

    /// Send a request to the server and collect the response
    async fn send_and_collect_request_raw(
        url: hyper::Uri,
        sender: &mut SendRequest<Empty<Bytes>>,
        method: &str,
    ) -> Result<impl Buf, ServerRequest> {
        tracing::trace!("Request: {method} {url}");
        // make request
        let res = Self::send_request_raw(url, sender, method).await?;

        tracing::debug!("Server returned {}", res.status());

        // asynchronously aggregate the chunks of the body
        let body = res
            .collect()
            .await
            .inspect(|b| tracing::trace!("Response body: {b:?}"))?
            .aggregate();

        Ok(body)
    }

    /// Make a URI from a path and query
    #[allow(clippy::missing_panics_doc)]
    fn make_uri<T>(&self, path_and_query: T) -> Result<hyper::Uri, InvalidParameter>
    where
        T: Into<PathAndQuery>,
    {
        hyper::Uri::builder()
            // Checked before, should never fail
            .scheme(self.url.scheme().unwrap().clone())
            .authority(self.url.authority().unwrap().clone())
            .path_and_query(path_and_query)
            .build()
            .inspect(|x| tracing::trace!("Made uri {x}"))
            .map_err(InvalidParameter::Url)
    }

    /// Get the server metadata
    pub async fn metadata(&self) -> ServerMetadata {
        self.shared.read().await.metadata.clone()
    }

    /// Get the last time metadata was fetched from the server
    pub async fn last_update(&self) -> Instant {
        self.shared.read().await.last_update
    }

    /// Send a request to the server
    pub async fn send_request<T>(
        &self,
        method: &str,
        path: T,
    ) -> Result<Response<Incoming>, ServerRequest>
    where
        T: Into<PathAndQuery> + Debug,
    {
        tracing::debug!("send_request {method} {path:?}");
        Self::send_request_raw(
            self.make_uri(path)?,
            &mut self.shared.write().await.sender,
            method,
        )
        .await
    }

    /// Prepare a request to be sent to the server
    pub fn prepare_request<T>(&self, method: &str, path: T) -> Result<Builder, ServerRequest>
    where
        T: Into<PathAndQuery>,
    {
        Ok(Self::request_builder(&self.make_uri(path)?, method))
    }

    /// Fetch metadata from server
    async fn fetch(&self) -> Result<(), ServerRequest> {
        let mut shared = self.shared.write().await;

        let body = Self::send_and_collect_request_raw(
            self.make_uri(PathAndQuery::from_static("/transfer/metadata"))?,
            &mut shared.sender,
            "GET",
        )
        .await?;
        let buf = body.chunk();

        // try to deserialize ServerMetadata from body
        let new_metadata = bitcode::deserialize(buf)?;
        shared.last_update = Instant::now();

        if shared.metadata != new_metadata {
            shared.metadata = new_metadata;
            tracing::trace!("New metadata: {:?}", shared.metadata);
        }

        Ok(())
    }

    /// Transfer chunks from server
    pub async fn transfer(&self, hash: &str) -> Result<Vec<OwnedHashTreeNode>, ServerRequest> {
        tracing::trace!("Preparing transfer request: target: {hash}");
        let mut shared = self.shared.write().await;

        let path_and_query = PathAndQuery::from_str(format!("/transfer/whole/{hash}").as_str())
            .map_err(InvalidParameter::Uri)?;

        let body = Self::send_and_collect_request_raw(
            self.make_uri(path_and_query)?,
            &mut shared.sender,
            "GET",
        )
        .await
        .inspect_err(|e| tracing::error!("Error during request: {e}"))?;
        let mut buf: Vec<u8> = vec![];
        body.reader().read_exact(&mut buf)?;

        tracing::trace!("Extracted {} bytes from body", buf.len());

        // try to deserialize ServerMetadata from body
        let chunks: Vec<OwnedHashTreeNode> = bitcode::deserialize(&buf)?;
        Ok(chunks)
    }

    // TODO diff may optionally be computed client-side
    /// Transfer chunks from server, computing diff from local data
    pub async fn transfer_diff(
        &self,
        hash: &str,
        from: Vec<Hash>,
    ) -> Result<OwnedHashTreeNode, ServerRequest> {
        tracing::trace!("Preparing transfer/diff request: target: {hash}, from:{from:?}");
        let mut shared = self.shared.write().await;

        // comma separated list of hashes
        let from = from
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<String>>()
            .join(",");

        let path_and_query =
            PathAndQuery::from_str(format!("/transfer/diff/{hash}?got={from}").as_str())
                .map_err(InvalidParameter::Uri)?;

        let body = Self::send_and_collect_request_raw(
            self.make_uri(path_and_query)?,
            &mut shared.sender,
            "GET",
        )
        .await
        .inspect_err(|e| tracing::error!("Error during request: {e}"))?;
        let mut buf: Vec<u8> = vec![];
        body.reader().read_to_end(&mut buf)?;

        tracing::trace!("Extracted {} bytes from body", buf.len());

        // try to deserialize ServerMetadata from body
        let chunks: OwnedHashTreeNode = bitcode::deserialize(&buf)?;
        Ok(chunks)
    }

    /// Fetch metadata from server in a loop
    pub async fn fetch_loop(self) {
        loop {
            tokio::time::sleep(self.timeout).await;
            if self.fetch().await.is_err() {
                // try to re-establish connection to server
                if let Ok(sender) = connection::make(self.url.clone()).await {
                    tracing::info!("Connected to server");
                    self.shared.write().await.sender = sender;
                } else {
                    tracing::warn!(
                        "Cannot connect to server, retrying in {} seconds",
                        self.timeout.as_secs()
                    );
                }
            }
        }
    }

    /// Create a new server instance
    ///
    /// # Arguments
    /// * `url` - server url
    /// * `pub_key` - client public key, TODO
    /// * `client_name` - client name
    /// * `timeout` - timeout for server fetches, TODO
    ///
    /// # Returns
    /// A new server instance
    ///
    /// # Errors
    /// * `ServerRequest::BadPubKey` - if the public key is invalid
    /// * `ServerRequest::Request` - if the request fails
    /// * `ServerRequest::Utf8` - if the response is not valid utf8
    /// * `ServerRequest::Uuid` - if the response is not a valid uuid
    ///
    /// # Panics
    /// * If the url does not have a scheme or authority
    pub async fn new(
        url: hyper::Uri,
        //pub_key: &PublicKey,
        client_name: &str,
        pub_key: &[u8; 32],
    ) -> Result<Self, ServerRequest> {
        // Uri values are not going to change so we'll check them now and then just unwrap
        assert!(url.scheme().is_some());
        assert!(url.authority().is_some());

        let sender = connection::make(url.clone()).await?;
        let timeout = Duration::new(5, 0); // TODO make this configurable

        let mut server = Self {
            pub_key: pub_key
                .as_ref()
                .try_into()
                .map_err(|_| ServerRequest::BadPubKey)?,
            url,
            client_uid: None,
            shared: Arc::new(RwLock::new(SharedServer {
                metadata: ServerMetadata::default(),
                sender,
                last_update: Instant::now(),
            })),
            timeout,
        };
        server.register(client_name).await?;
        server.fetch().await?;

        Ok(server)
    }

    /// Register a new client
    pub async fn register(&mut self, client_name: &str) -> Result<Uuid, ServerRequest> {
        use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
        let request = self
            .prepare_request(
                "POST",
                PathAndQuery::try_from(format!(
                    "/clients?name={}&version={}",
                    utf8_percent_encode(client_name, NON_ALPHANUMERIC),
                    *VERSION,
                ))
                .map_err(InvalidParameter::Uri)?,
            )?
            .body(Empty::<Bytes>::new())?;

        let mut res = self
            .shared
            .write()
            .await
            .sender
            .send_request(request)
            .await?;

        tracing::trace!("Received response {}", res.status());
        let collected = res.body_mut().collect().await?;

        let b = collected.to_bytes();
        let uid_str = std::str::from_utf8(&b)?;
        let uid = Uuid::parse_str(uid_str)?;
        tracing::info!("Got uuid '{uid:?}' from server");
        self.client_uid = Some(uid);

        Ok(uid)
    }
}
