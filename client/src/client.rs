use std::collections::HashMap;

use anyhow::Error;

use distd_core::{chunk_storage::ChunkStorage, metadata::ItemMetadata};

use crate::server::Server;

#[derive(Debug)]
pub struct RegisterError;

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
    pub items: HashMap<String, ItemMetadata>,
}

impl<T> Client<T>
where
    T: ChunkStorage,
{
    pub async fn new(
        client_name: &str,
        server_addr: hyper::Uri,
        server_public_key: &[u8; 32],
        storage: T,
    ) -> Result<Self, Error> {
        Server::new(server_addr, client_name, server_public_key)
            .await
            .map_err(Error::msg)
            .map(|server| Self {
                name: String::from(client_name),
                server,
                storage,
                items: HashMap::default(),
            })
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
