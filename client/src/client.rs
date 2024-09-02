use anyhow::Error;

use distd_core::chunk_storage::ChunkStorage;

use crate::server::Server;

#[derive(Debug)]
pub struct RegisterError;

pub struct Client<T>
where
    T: ChunkStorage,
{
    name: String,
    /// Associated server
    pub server: Server,
    pub storage: T,
}

impl<T> Client<T>
where
    T: ChunkStorage,
{
    pub async fn new(
        server_addr: hyper::Uri,
        server_public_key: &[u8; 32],
        storage: T,
    ) -> Result<Self, Error> {
        Server::new(server_addr, "Some name", server_public_key)
            .await
            .map_err(Error::msg)
            .map(|server| Self {
                name: String::from("hostname"),
                server,
                storage,
            })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
