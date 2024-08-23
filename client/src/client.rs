use crate::server::Server;

#[derive(Debug)]
pub struct RegisterError;

pub struct Client {
    /// Associated server
    pub server: Server,
}

impl Client {
    pub async fn new(
        server_addr: hyper::Uri,
        server_public_key: &[u8; 32],
    ) -> Self {
        let server = Server::new(server_addr, server_public_key).await.unwrap();
        Self { server }
    }
}
