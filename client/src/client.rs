use crate::server::Server;

#[derive(Debug)]
pub struct RegisterError;

pub struct Client{
    server: Server,
}
