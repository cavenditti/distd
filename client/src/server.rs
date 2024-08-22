//use std::{net::SocketAddr
use anyhow::Error;
use std::time::Instant;

use http_body_util::{BodyExt, Empty};
use hyper::{
    body::{Buf, Bytes, Incoming},
    client::conn::http1::SendRequest,
    Request, Response,
};
use tokio::sync::RwLock;

//use ring::agreement::PublicKey;

use distd_core::metadata::ServerMetadata;

use crate::connection;

/// Server representation used by clients
pub struct Server {
    //pub connection: ..

    // server address
    //pub addr: SocketAddr,
    /// server url
    pub url: hyper::Uri,

    /// server Ed25519 public key
    pub pub_key: [u8; 32], // TODO Check this

    /// global server metadata
    pub metadata: RwLock<ServerMetadata>,

    /// last time metadata was fetched from server
    pub last_update: Instant,

    /// sender for REST requests to server
    pub sender: SendRequest<Empty<Bytes>>,
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

    fn make_uri(&self, path_and_query: &str) -> Result<hyper::Uri, Error> {
        hyper::Uri::builder()
            .scheme(self.url.scheme().unwrap().clone())
            .authority(self.url.authority().unwrap().clone())
            .path_and_query(path_and_query)
            .build()
            .map_err(Error::msg)
    }

    pub async fn send_request(
        &mut self,
        method: &str,
        path: &str,
    ) -> Result<Response<Incoming>, Error> {
        Self::_send_request(self.make_uri(path)?, &mut self.sender, method).await
    }

    async fn fetch(&mut self) -> Result<(), Error> {
        let mut metadata = self.metadata.write().await;

        let body = Self::_send_and_collect_request(
            self.make_uri("/transfer/metadata")?,
            &mut self.sender,
            "GET",
        )
        .await?;
        let buf = body.chunk();

        // try to deserialize ServerMetadata from body
        let new_metadata = bitcode::deserialize(buf).map_err(Error::msg)?;

        *metadata = new_metadata;
        Ok(())
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

        let mut server = Self {
            pub_key: pub_key.as_ref().try_into().unwrap(), // FIXME
            url,
            metadata: RwLock::new(ServerMetadata::default()),
            sender,
            last_update: Instant::now(),
        };
        server.fetch().await?;
        Ok(server)
    }
}
