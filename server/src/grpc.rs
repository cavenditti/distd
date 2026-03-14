use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use distd_core::chunk_storage::ChunkStorage;
use distd_core::proto::{self, SyncMessage};
use distd_core::proto::sync_message::Msg;
use distd_core::utils::grpc::metadata_to_uuid;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Code, Request, Response, Status, Streaming};

use distd_core::proto::{distd_server::Distd, ClientKeepAlive, ClientRegister, ServerMetadata};
use uuid::Uuid;
use crate::error::Server as ServerError;
use crate::Server;

#[derive(Debug, Default, Clone)]
pub struct UuidAuthInterceptor {
    pub uuids: Arc<RwLock<HashSet<MetadataValue<distd_core::tonic::metadata::Binary>>>>,
}

impl Interceptor for UuidAuthInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        match request.borrow().metadata().get_bin("x-uuid-bin") {
            Some(uuid) => {
                let parsed = metadata_to_uuid(uuid)
                    .map_err(|_| Status::unauthenticated("Invalid metadata"))?;

                if parsed.is_nil() || self.uuids.read().unwrap().contains(uuid) {
                    Ok(request)
                } else {
                    Err(Status::unauthenticated("Unauthenticated"))
                }
            }
            None => Ok(request),
        }
    }
}

fn ensure_authenticated(
    request: &Request<impl Sized>,
    interceptor: &UuidAuthInterceptor,
) -> Result<Uuid, Status> {
    let uuid = request
        .metadata()
        .get_bin("x-uuid-bin")
        .ok_or_else(|| Status::unauthenticated("Unauthenticated"))?;
    let parsed = metadata_to_uuid(uuid).map_err(|_| Status::unauthenticated("Invalid metadata"))?;

    if parsed.is_nil() || !interceptor.uuids.read().unwrap().contains(uuid) {
        return Err(Status::unauthenticated("Unauthenticated"));
    }

    Ok(parsed)
}

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    pub async fn make_grpc_service(self) -> Result<tonic::transport::server::Router, ServerError> {
        let interceptor = self.uuid_interceptor.clone();
        let inner = proto::distd_server::DistdServer::new(self)
            .max_decoding_message_size(256 * 1024 * 1024)
            .max_encoding_message_size(256 * 1024 * 1024);
        let svc = tonic::service::interceptor::InterceptedService::new(inner, interceptor);

        Ok(tonic::transport::Server::builder().add_service(svc))
    }
}

type SyncResponseStream = std::pin::Pin<Box<dyn Stream<Item = Result<SyncMessage, Status>> + Send>>;

#[tonic::async_trait]
impl<T> Distd for Server<T>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    type SyncStream = SyncResponseStream;

    async fn register(
        &self,
        request: Request<ClientRegister>,
    ) -> Result<Response<ServerMetadata>, Status> {
        let addr = request.remote_addr();
        let inner = request.into_inner();
        let addr = addr.ok_or(Status::new(Code::Internal, "Invalid source address"))?;
        let response = self
            .register_response(addr, inner)
            .await
            .map_err(|message| Status::new(Code::Internal, message))?;
        Ok(Response::new(response))
    }

    async fn fetch(
        &self,
        request: Request<ClientKeepAlive>,
    ) -> Result<Response<ServerMetadata>, Status> {
        ensure_authenticated(&request, &self.uuid_interceptor)?;
        let response = self
            .fetch_response(request.into_inner())
            .await
            .map_err(|message| Status::new(Code::Internal, message))?;
        Ok(Response::new(response))
    }

    /// PPSPP-style bidirectional sync.
    ///
    /// Protocol:
    /// 1. Client sends ManifestRequest
    /// 2. Server sends ManifestResponse (with ordered chunk hashes)
    /// 3. Client sends PossessionBitfield on the same stream
    /// 4. Server streams ChunkData (or BulkData) for missing chunks
    async fn sync(
        &self,
        request: Request<Streaming<SyncMessage>>,
    ) -> Result<Response<SyncResponseStream>, Status> {
        // We cannot call ensure_authenticated on streaming requests easily,
        // so we rely on the interceptor having already validated the UUID.

        let mut in_stream = request.into_inner();
        let server = self.clone();

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            // Phase 1: wait for ManifestRequest
            let manifest_req = match in_stream.next().await {
                Some(Ok(SyncMessage { msg: Some(Msg::ManifestRequest(req)) })) => req,
                _ => {
                    let _ = tx.send(Err(Status::invalid_argument("Expected ManifestRequest"))).await;
                    return;
                }
            };

            let (manifest_response, item) = match server
                .sync_manifest_response(manifest_req)
                .await
            {
                Ok(response) => response,
                Err(message) => {
                    let status = if message.starts_with("Unknown artifact:") {
                        Status::not_found(message)
                    } else {
                        Status::internal(message)
                    };
                    let _ = tx.send(Err(status)).await;
                    return;
                }
            };

            if tx
                .send(Ok(SyncMessage {
                    msg: Some(Msg::ManifestResponse(manifest_response)),
                }))
                .await
                .is_err()
            {
                return;
            }

            // Phase 2: wait for PossessionBitfield
            let possession = match in_stream.next().await {
                Some(Ok(SyncMessage { msg: Some(Msg::Possession(poss)) })) => poss,
                _ => {
                    let _ = tx.send(Err(Status::invalid_argument("Expected PossessionBitfield"))).await;
                    return;
                }
            };

            match server
                .sync_chunk_response_messages(&item, possession)
                .await
            {
                Ok(messages) => {
                    for message in messages {
                        if tx.send(Ok(message)).await.is_err() {
                            return;
                        }
                    }
                }
                Err(message) => {
                    let status = if message.starts_with("Unknown artifact:") {
                        Status::not_found(message)
                    } else if message == "Invalid bitfield" {
                        Status::invalid_argument(message)
                    } else {
                        Status::internal(message)
                    };
                    let _ = tx.send(Err(status)).await;
                    return;
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::SyncStream))
    }
}
