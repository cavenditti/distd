//use std::{net::SocketAddr
use anyhow::Error;
use std::time::Duration;

use http_body_util::{BodyExt, Empty};
use hyper::{
    body::{Buf, Bytes, Incoming},
    client::conn::http1::SendRequest,
    http::uri::PathAndQuery,
    Request, Response,
};
use tokio::{sync::RwLock, time::Instant};

//use ring::agreement::PublicKey;

use distd_core::metadata::ServerMetadata;

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

    /// Shared data
    shared: RwLock<SharedServer>,

    /// Elapsed time between server fetches
    timeout: Duration,
}

impl Server {
    async fn _send_request(
        url: hyper::Uri,
        sender: &mut SendRequest<Empty<Bytes>>,
        method: &str,
    ) -> Result<Response<Incoming>, Error> {
        // Prepare request
        let req = Request::builder()
            .uri(url.clone())
            .method(method)
            .header(hyper::header::HOST, url.authority().unwrap().as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|_| Error::msg("Cannot build request body"))?;

        // Fetch from url
        let res = sender
            .send_request(req)
            .await
            .map_err(|_| Error::msg("Cannot complete request"))?;

        Ok(res)
    }

    async fn _send_and_collect_request(
        url: hyper::Uri,
        sender: &mut SendRequest<Empty<Bytes>>,
        method: &str,
    ) -> Result<impl Buf, Error> {
        // make request
        let res = Self::_send_request(url, sender, method).await?;

        // asynchronously aggregate the chunks of the body
        let body = res
            .collect()
            .await
            .map_err(|_| Error::msg("Cannot collect response"))?
            .aggregate();

        Ok(body)
    }

    fn make_uri<T>(&self, path_and_query: T) -> Result<hyper::Uri, Error>
    where
        T: Into<PathAndQuery>,
    {
        hyper::Uri::builder()
            .scheme(self.url.scheme().unwrap().clone())
            .authority(self.url.authority().unwrap().clone())
            .path_and_query(path_and_query)
            .build()
            .map_err(Error::msg)
    }

    pub async fn get_metadata(&self) -> ServerMetadata {
        self.shared.read().await.metadata.clone()
    }

    pub async fn get_last_update(&self) -> Instant {
        self.shared.read().await.last_update
    }

    pub async fn send_request<T>(&self, method: &str, path: T) -> Result<Response<Incoming>, Error>
    where
        T: Into<PathAndQuery>,
    {
        Self::_send_request(
            self.make_uri(path)?,
            &mut self.shared.write().await.sender,
            method,
        )
        .await
    }

    async fn fetch(&self) -> Result<(), Error> {
        let mut shared = self.shared.write().await;

        let body = Self::_send_and_collect_request(
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
            println!("New metadata: {:?}", shared.metadata);
        } else {
            println!("Metadata didn't change.");
        }
        Ok(())
    }

    pub async fn fetch_loop(&self) {
        loop {
            tokio::time::sleep(self.timeout).await;
            if self.fetch().await.is_err() {
                break;
            }
        }
        panic!("Cannot fetch from server") // FIXME
    }

    pub async fn new(
        url: hyper::Uri,
        //pub_key: &PublicKey,
        pub_key: &[u8; 32],
    ) -> Result<Self, Error> {
        // Uri values are not going to change so we'll check them now and then just unwrap
        assert!(url.scheme().is_some());
        assert!(url.authority().is_some());

        let sender = connection::make_connection(url.clone()).await?;
        let timeout = Duration::new(5, 0); // TODO make this configurable

        let server = Self {
            pub_key: pub_key.as_ref().try_into().unwrap(), // FIXME
            url,
            shared: RwLock::new(SharedServer {
                metadata: ServerMetadata::default(),
                sender,
                last_update: Instant::now(),
            }),
            timeout,
        };
        server.fetch().await?;

        Ok(server)
    }
}
