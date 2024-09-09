//use std::{net::SocketAddr
use anyhow::Error;
use blake3::Hash;
use std::{fmt::Debug, io::Read, str::FromStr, time::Duration};
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
    shared: RwLock<SharedServer>,

    /// Elapsed time between server fetches
    timeout: Duration,
}

impl Server {
    fn request_builder(url: hyper::Uri, method: &str) -> Builder {
        Request::builder()
            .uri(url.clone())
            .method(method)
            .header(hyper::header::HOST, url.authority().unwrap().as_str())
    }

    async fn send_request_raw(
        url: hyper::Uri,
        sender: &mut SendRequest<Empty<Bytes>>,
        method: &str,
    ) -> Result<Response<Incoming>, Error> {
        // Prepare request
        let request = Self::request_builder(url, method)
            .body(Empty::<Bytes>::new())
            .map_err(|_| Error::msg("Cannot build request body"))?;

        // Fetch from url
        let res = sender
            .send_request(request)
            .await
            .map_err(|_| Error::msg("Cannot complete request"))?;

        Ok(res)
    }

    async fn send_and_collect_request_raw(
        url: hyper::Uri,
        sender: &mut SendRequest<Empty<Bytes>>,
        method: &str,
    ) -> Result<impl Buf, Error> {
        tracing::trace!("Request: {method} {url}");
        // make request
        let res = Self::send_request_raw(url, sender, method).await?;

        tracing::debug!("Server returned {}", res.status());

        // asynchronously aggregate the chunks of the body
        let body = res
            .collect()
            .await
            .inspect(|b| tracing::trace!("Response body: {b:?}"))
            .map_err(|_| Error::msg("Cannot collect response"))?
            .aggregate();

        Ok(body)
    }

    #[allow(clippy::missing_panics_doc)]
    fn make_uri<T>(&self, path_and_query: T) -> Result<hyper::Uri, Error>
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
            .map_err(Error::msg)
    }

    pub async fn metadata(&self) -> ServerMetadata {
        self.shared.read().await.metadata.clone()
    }

    pub async fn last_update(&self) -> Instant {
        self.shared.read().await.last_update
    }

    pub async fn send_request<T>(&self, method: &str, path: T) -> Result<Response<Incoming>, Error>
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

    pub async fn prepare_request<T>(&self, method: &str, path: T) -> Result<Builder, Error>
    where
        T: Into<PathAndQuery>,
    {
        Ok(Self::request_builder(self.make_uri(path)?, method))
    }

    async fn fetch(&self) -> Result<(), Error> {
        let mut shared = self.shared.write().await;

        let body = Self::send_and_collect_request_raw(
            self.make_uri(PathAndQuery::from_static("/transfer/metadata"))?,
            &mut shared.sender,
            "GET",
        )
        .await?;
        let buf = body.chunk();

        // try to deserialize ServerMetadata from body
        let new_metadata = bitcode::deserialize(buf).map_err(Error::msg)?;
        shared.last_update = Instant::now();

        if shared.metadata != new_metadata {
            shared.metadata = new_metadata;
            tracing::trace!("New metadata: {:?}", shared.metadata);
        } else {
            tracing::trace!("Metadata didn't change.");
        }
        Ok(())
    }

    pub async fn transfer(&self, hash: &str) -> Result<Vec<OwnedHashTreeNode>, Error> {
        tracing::trace!("Preparing transfer request: target: {hash}");
        let mut shared = self.shared.write().await;

        let path_and_query = PathAndQuery::from_str(format!("/transfer/whole/{hash}").as_str())
            .map_err(Error::msg)?;

        let body = Self::send_and_collect_request_raw(
            self.make_uri(path_and_query)?,
            &mut shared.sender,
            "GET",
        )
        .await
        .inspect_err(|e| tracing::error!("Error during request: {e}"))?;
        let mut buf: Vec<u8> = vec![];
        body.reader().read(&mut buf).map_err(Error::msg)?;

        tracing::trace!("Extracted {} bytes from body", buf.len());

        // try to deserialize ServerMetadata from body
        let chunks: Vec<OwnedHashTreeNode> = bitcode::deserialize(&buf).map_err(Error::msg)?;
        Ok(chunks)
    }

    // TODO diff may optionally be computed client-side
    /// Transfer chunks from server, computing diff from local data
    pub async fn transfer_diff(
        &self,
        hash: &str,
        from: Vec<Hash>,
    ) -> Result<Vec<OwnedHashTreeNode>, Error> {
        tracing::trace!("Preparing transfer/diff request: target: {hash}, from:{from:?}");
        let mut shared = self.shared.write().await;

        // comma separated list of hashes
        let from = from
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<String>>()
            .join(",");

        let from_query = if from.is_empty() {
            ""
        } else {
            &format!("?from={from}")
        };

        let path_and_query =
            PathAndQuery::from_str(format!("/transfer/diff/{hash}{from_query}").as_str())
                .map_err(Error::msg)?;

        let body = Self::send_and_collect_request_raw(
            self.make_uri(path_and_query)?,
            &mut shared.sender,
            "GET",
        )
        .await
        .inspect_err(|e| tracing::error!("Error during request: {e}"))?;
        let mut buf: Vec<u8> = vec![];
        body.reader().read_to_end(&mut buf).map_err(Error::msg)?;

        tracing::trace!("Extracted {} bytes from body", buf.len());

        // try to deserialize ServerMetadata from body
        let chunks: Vec<OwnedHashTreeNode> = bitcode::deserialize(&buf).map_err(Error::msg)?;
        Ok(chunks)
    }

    pub async fn fetch_loop(&self) {
        loop {
            tokio::time::sleep(self.timeout).await;
            if self.fetch().await.is_err() {
                // try to re-establish connection to server
                if let Ok(sender) = connection::make_connection(self.url.clone()).await {
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

    pub async fn new(
        url: hyper::Uri,
        //pub_key: &PublicKey,
        client_name: &str,
        pub_key: &[u8; 32],
    ) -> Result<Self, Error> {
        // Uri values are not going to change so we'll check them now and then just unwrap
        assert!(url.scheme().is_some());
        assert!(url.authority().is_some());

        let sender = connection::make_connection(url.clone()).await?;
        let timeout = Duration::new(5, 0); // TODO make this configurable

        let mut server = Self {
            pub_key: pub_key.as_ref().try_into().unwrap(), // FIXME
            url,
            client_uid: None,
            shared: RwLock::new(SharedServer {
                metadata: ServerMetadata::default(),
                sender,
                last_update: Instant::now(),
            }),
            timeout,
        };
        server.register(client_name).await?;
        server.fetch().await?;

        Ok(server)
    }

    pub async fn register(&mut self, client_name: &str) -> Result<Uuid, Error> {
        use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
        let req = self
            .prepare_request(
                "POST",
                PathAndQuery::try_from(format!(
                    "/clients?name={}&version={}",
                    utf8_percent_encode(client_name, NON_ALPHANUMERIC),
                    *VERSION,
                ))
                .unwrap(),
            )
            .await?
            .body(Empty::<Bytes>::new())
            .map_err(Error::msg)?;

        let mut res = self
            .shared
            .write()
            .await
            .sender
            .send_request(req)
            .await
            .map_err(|_| Error::msg("Cannot complete request"))?;

        tracing::trace!("Received response {}", res.status());
        res.body_mut()
            .collect()
            .await
            .map_err(Error::msg)
            .and_then(|x| {
                std::str::from_utf8(&x.to_bytes())
                    .map_err(Error::msg)
                    .and_then(|x| Uuid::parse_str(x).map_err(Error::msg))
            })
            .inspect(|uid| {
                tracing::info!("Got uuid '{uid:?}' from server");
                self.client_uid = Some(*uid);
            })
    }
}
