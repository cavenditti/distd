use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::extract::FromRef;
use distd_core::chunk_storage::node_stream::sender;
use distd_core::chunk_storage::ChunkStorage;
use distd_core::hash::Hash;
use distd_core::proto::{self, EnumAcknowledge, ItemRequest, SerializedTree};
use distd_core::utils::grpc::metadata_to_uuid;
use distd_core::utils::serde::BitcodeSerializable;
use distd_core::utils::uuid::{bytes_to_uuid, slice_to_uuid};
use distd_core::version::Version;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

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
    T: ChunkStorage + Sync + Send + Default + Debug + 'static,
{
    pub async fn make_grcp_service(self) -> Result<tonic::transport::server::Router, ServerError> {
        let interceptor = self.uuid_interceptor.clone();
        let svc = proto::distd_server::DistdServer::with_interceptor(self, interceptor);

        Ok(tonic::transport::Server::builder().add_service(svc))
    }
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<SerializedTree, Status>> + Send>>;

#[tonic::async_trait]
impl<T> Distd for Server<T>
where
    T: ChunkStorage + Sync + Send + Default + Debug + 'static,
{
    type TreeTransferStream = ResponseStream;

    async fn register(
        &self,
        request: Request<ClientRegister>,
    ) -> Result<Response<ServerMetadata>, Status> {
        let addr = request.remote_addr().clone();
        let inner = request.into_inner();
        let addr = addr.ok_or(Status::new(Code::Internal, "Invalid source address"))?;
        let uuid = self
            .register_client(
                inner.name,
                addr,
                Version::from_str(&inner.version).ok(),
                inner.uuid.map(|x| slice_to_uuid(&x)),
            )
            .await
            .map_err(|_| Status::new(Code::Internal, "Cannot assign new UUID"))?;
        let serialized = ServerMetadataRepr::from(self.metadata.read().await.clone())
            .to_bitcode()
            .map_err(|_| Status::new(Code::Internal, "Cannot serialize server metadata"))?;
        Ok(Response::new(ServerMetadata {
            serialized,
            uuid: Some(uuid.as_bytes().to_vec()),
        }))
    }

    async fn fetch(
        &self,
        _request: Request<ClientKeepAlive>,
    ) -> Result<Response<ServerMetadata>, Status> {
        let serialized = ServerMetadataRepr::from(self.metadata.read().await.clone())
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
    ) -> Result<Response<ResponseStream>, Status> {
        let inner = request.into_inner();

        let hash = {
            let metadata = self.metadata.read().await;
            *PathBuf::from_str(&inner.item_path)
                .ok()
                .and_then(|x| metadata.items.get(&x))
                .ok_or(Status::new(
                    Code::InvalidArgument,
                    "Bad or missing item path",
                ))?
                .root()
        };

        tracing::debug!("Transfer {hash}");

        let from = inner.hashes.unwrap_or_default();
        let from: Vec<Hash> = from
            .hashes
            .into_iter()
            .flat_map(|v| {
                v.try_into()
                    .map_err(|_| Status::new(Code::InvalidArgument, "Bad BLAKE3 hash"))
            })
            .map(Hash::from_bytes)
            .collect();

        let nodes = self
            .storage
            .read()
            .await
            .get(&hash)
            .ok_or(Status::new(Code::NotFound, "tree not found"))?
            .find_diff(&from)
            .inspect(|hs| tracing::trace!("Transferring chunks: {hs}"));
        /*
            .map(|n| bitcode::serialize(&n))
            .map(|n| {
                n.map(|inner| SerializedTree {
                    payload: inner,
                })
                .inspect_err(|e| tracing::error!("Cannot serialize chunk {}", e))
                .map_err(|_| Status::new(Code::Internal, "Cannot serialize"))
            });
        //.flatten(); // FIXME this ignores any error, it's unwrapped down here but equally bad
        */

        //let mut stream = Box::pin(tokio_stream::iter(nodes).throttle(Duration::from_millis(200)));
        // FIXME make serialization fail gracefully instead of panicking
        // This is due to the Results in the Iterator having to be checked one by one
        let stream = Box::pin(tokio_stream::iter(nodes));
        let mut stream =
            sender(stream, 32, Duration::new(0, 4800)).map(|x| SerializedTree { payload: x });

        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {
                        // item (serialized tree) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            tracing::trace!("\tclient disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::TreeTransferStream
        ))
    }
}
