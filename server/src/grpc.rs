use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use distd_core::chunk_storage::ChunkStorage;
use distd_core::hash::Hash;
use distd_core::proto::{self, EnumAcknowledge, ItemRequest, SerializedTree};
use distd_core::utils::grpc::metadata_to_uuid;
use distd_core::utils::serde::BitcodeSerializable;
use distd_core::version::Version;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Code, Request, Response, Status};

use distd_core::metadata::Server as ServerMetadataRepr;
use distd_core::proto::{
    distd_server::Distd, Acknowledge, ClientKeepAlive, ClientRegister, Hashes, ServerMetadata,
};
use uuid::Uuid;

use crate::error::Server as ServerError;
use crate::Server;

#[derive(Clone)]
struct ClientUuidExtension {
    pub uuid: Uuid,
}

#[derive(Debug, Default, Clone)]
pub struct UuidAuthInterceptor {
    pub uuids: Arc<RwLock<HashSet<MetadataValue<distd_core::tonic::metadata::Binary>>>>,
}

impl Interceptor for UuidAuthInterceptor {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        let uuid: MetadataValue<tonic::metadata::Binary>;
        {
            uuid = match request.borrow().metadata().get_bin("x-uuid-bin") {
                Some(uuid) if self.uuids.read().unwrap().contains(uuid) => Ok(uuid),
                _ => Err(Status::unauthenticated("Unauthenticated")),
            }?
            .clone();
        }

        request.extensions_mut().insert(ClientUuidExtension {
            uuid: metadata_to_uuid(&uuid)
                //.map_err(|_| Status::unauthenticated("Unauthenticated"))?,
                .map_err(|_| Status::unauthenticated("Invalid metadata"))?,
        });

        Ok(request)
    }
}

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Clone + Default + Debug + 'static,
{
    pub async fn make_grcp_service(self) -> Result<tonic::transport::server::Router, ServerError> {
        let interceptor = self.uuid_interceptor.clone();
        let svc = proto::distd_server::DistdServer::with_interceptor(self, interceptor);

        Ok(tonic::transport::Server::builder().add_service(svc))
    }
}

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
        let hash = Hash::from_bytes(hash);
        tracing::debug!("Transfer {hash}");

        let from = inner.hashes.unwrap_or_default();
        let from = from
            .hashes
            .into_iter()
            .flat_map(|v| {
                v.try_into()
                    .map_err(|_| Status::new(Code::InvalidArgument, "Bad BLAKE3 hash"))
            })
            .map(Hash::from_bytes);

        let from: Vec<Hash> = from.collect();

        let tree_diff = self
            .storage
            .diff_tree(&hash, &from)
            .ok_or(Status::new(Code::NotFound, "tree not found"))
            .inspect(|hs| tracing::trace!("Transferring chunks: {hs}"))
            .inspect_err(|_| tracing::warn!("Cannot find hash {hash}"))?;

        let bitcode_hashtree: Vec<u8> = bitcode::serialize(&tree_diff)
            .inspect(|s| tracing::debug!("Serialized size: {}B", s.len()))
            .inspect_err(|e| tracing::error!("Cannot serialize chunk {}", e))
            .map_err(|_| Status::new(Code::Internal, "Cannot serialize"))?;

        Ok(Response::new(SerializedTree { bitcode_hashtree }))
    }
}
