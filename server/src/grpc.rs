use std::fmt::Debug;
use std::str::FromStr;

use distd_core::chunk_storage::ChunkStorage;
use distd_core::proto::{EnumAcknowledge, ItemRequest, SerializedTree};
use distd_core::utils::serde::BitcodeSerializable;
use distd_core::version::Version;
use tonic::{Code, Request, Response, Status};

use distd_core::metadata::Server as ServerMetadataRepr;
use distd_core::proto::{
    distd_server::Distd, Acknowledge, ClientKeepAlive, ClientRegister, Hashes, ServerMetadata,
};

use crate::Server;

#[tonic::async_trait]
impl<T> Distd for Server<T>
where
    T: ChunkStorage + Sync + Send + Clone + Default + Debug + 'static,
{
    async fn register(
        &self,
        request: Request<ClientRegister>,
    ) -> Result<Response<ServerMetadata>, Status> {
        let addr = request.remote_addr().clone();
        let inner = request.into_inner();
        let addr = addr.ok_or(Status::new(Code::Internal, "Invalid source address"))?;
        let uuid = self
            .register_client(inner.name, addr, Version::from_str(&inner.version).ok())
            .map_err(|_| Status::new(Code::Internal, "Cannot assign new UUID"))?;
        let serialized = ServerMetadataRepr::from(self.metadata.read().unwrap().clone())
            .to_bitcode()
            .map_err(|_| Status::new(Code::Internal, "Cannot serialize server metadata"))?;
        Ok(Response::new(ServerMetadata {
            serialized,
            uuid: Some(uuid.to_bytes_le().to_vec()),
        }))
    }

    async fn fetch(
        &self,
        _request: Request<ClientKeepAlive>,
    ) -> Result<Response<ServerMetadata>, Status> {
        let serialized = ServerMetadataRepr::from(self.metadata.read().unwrap().clone())
            .to_bitcode()
            .map_err(|_| Status::new(Code::Internal, "Cannot serialize server metadata"))?;
        Ok(Response::new(ServerMetadata {
            serialized,
            uuid: None, // TODO respond with the request uuid?
        }))
    }

    async fn adv_hashes(&self, _request: Request<Hashes>) -> Result<Response<Acknowledge>, Status> {
        Ok(Response::new(Acknowledge {
            ack: EnumAcknowledge::AckIgnored.into(),
        }))
    }

    async fn tree_transfer(
        &self,
        request: Request<ItemRequest>,
    ) -> Result<Response<SerializedTree>, Status> {
        let inner = request.into_inner();

        let hash: [u8; 32] = inner
            .item_root
            .try_into()
            .map_err(|_| Status::new(Code::InvalidArgument, "Bad BLAKE3 hash"))?;
        let hash = blake3::Hash::from_bytes(hash);
        tracing::debug!("Transfer {hash}");

        let from = inner.hashes.unwrap_or_default();
        let from: Result<Vec<[u8; 32]>, Status> = from
            .hashes
            .into_iter()
            .map(|v| {
                v.try_into()
                    .map_err(|_| Status::new(Code::InvalidArgument, "Bad BLAKE3 hash"))
            })
            .collect();
        let from: Vec<blake3::Hash> = from?.into_iter().map(blake3::Hash::from_bytes).collect();
        tracing::trace!("Client signals it has already {from:?}");

        let tree_diff = self
            .storage
            .diff_tree(&hash, &from)
            .ok_or(Status::new(Code::NotFound, "tree not found"))
            .inspect(|hs| tracing::debug!("Transferring chunks: {hs}"))
            .inspect_err(|_| tracing::warn!("Cannot find hash {hash}"))?;

        let bitcode_hashtree: Vec<u8> = bitcode::serialize(&tree_diff)
            .inspect(|s| tracing::debug!("Serialized size: {}B", s.len()))
            .inspect_err(|e| tracing::error!("Cannot serialize chunk {}", e))
            .map_err(|_| Status::new(Code::Internal, "Cannot serialize"))?;

        Ok(Response::new(SerializedTree { bitcode_hashtree }))
    }
}
