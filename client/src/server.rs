//use std::{net::SocketAddr
use std::{sync::RwLock, time::Instant};

use http_body_util::{BodyExt, Empty};
use hyper::{
    body::{Buf, Bytes, Incoming},
    client::conn::http2::SendRequest,
    Request, Response,
};

use ring::agreement::PublicKey;

use distd_core::metadata::ServerMetadata;

/// Server representation used by clients
pub struct Server {
    //pub connection: ..

    // server address
    //pub addr: SocketAddr,
    // server url
    pub addr: hyper::Uri,
    // server Ed25519 public key
    pub pub_key: [u8; 32], // TODO Check this
    // global server metadata
    pub metadata: RwLock<ServerMetadata>,
    // last time metadata was fetched from server
    pub last_update: Instant,
}

impl Server {
    async fn send_request(
        addr: hyper::Uri,
        mut sender: SendRequest<Empty<Bytes>>,
    ) -> Result<impl Buf, anyhow::Error> {
        // Prepare request
        let req = Request::builder()
            .uri(addr.clone())
            .header(hyper::header::HOST, addr.authority().unwrap().as_str())
            .body(Empty::<Bytes>::new())
            .map_err(|_| anyhow::Error::msg("Cannot build request body"))?;

        // Fetch from url
        let res = sender
            .send_request(req)
            .await
            .map_err(|_| anyhow::Error::msg("Cannot complete request"))?;

        // asynchronously aggregate the chunks of the body
        let body = res
            .collect()
            .await
            .map_err(|_| anyhow::Error::msg("Cannot collect response"))?
            .aggregate();

        // try to deserialize ServerMetadata from body
        Ok(body)
    }

    async fn fetch(&self, sender: SendRequest<Empty<Bytes>>) -> Result<(), anyhow::Error> {
        let mut metadata = self.metadata.write().unwrap(); //FIXME

        let body = Self::send_request(self.addr.clone(), sender).await?;
        let buf = body.chunk();

        // try to deserialize ServerMetadata from body
        let new_metadata = bitcode::deserialize(buf).map_err(|e| anyhow::Error::msg(e))?;

        *metadata = new_metadata;
        Ok(())
    }

    pub async fn new(
        addr: hyper::Uri,
        sender: SendRequest<Empty<Bytes>>,
        pub_key: &PublicKey,
    ) -> Result<Self, anyhow::Error> {
        let server = Self {
            pub_key: pub_key.as_ref().try_into().unwrap(), // FIXME
            addr,
            metadata: RwLock::new(ServerMetadata::default()),
            last_update: Instant::now(),
        };
        server.fetch(sender).await?;
        Ok(server)
    }
}
