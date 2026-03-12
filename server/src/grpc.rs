use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use distd_core::chunk_storage::ChunkStorage;
use distd_core::possession::Bitfield;
use distd_core::proto::{self, SyncMessage};
use distd_core::proto::sync_message::Msg;
use distd_core::utils::grpc::metadata_to_uuid;
use distd_core::utils::serde::BitcodeSerializable;
use distd_core::utils::uuid::slice_to_uuid;
use distd_core::version::Version;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};

use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::{Code, Request, Response, Status, Streaming};

use distd_core::metadata::Server as ServerMetadataRepr;
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
        request: Request<ClientKeepAlive>,
    ) -> Result<Response<ServerMetadata>, Status> {
        ensure_authenticated(&request, &self.uuid_interceptor)?;
        let serialized = ServerMetadataRepr::from(self.metadata.read().await.clone())
            .to_bitcode()
            .map_err(|_| Status::new(Code::Internal, "Cannot serialize server metadata"))?;
        Ok(Response::new(ServerMetadata {
            serialized,
            uuid: None,
        }))
    }

    /// PPSPP-style bidirectional sync.
    ///
    /// Protocol:
    /// 1. Client sends ManifestRequest
    /// 2. Server sends ManifestResponse (with ordered chunk hashes)
    /// 3. Client sends PossessionBitfield
    /// 4. Server streams ChunkData (or BulkData) for missing chunks
    async fn sync(
        &self,
        request: Request<Streaming<SyncMessage>>,
    ) -> Result<Response<SyncResponseStream>, Status> {
        // We cannot call ensure_authenticated on streaming requests easily,
        // so we rely on the interceptor having already validated the UUID.

        let mut in_stream = request.into_inner();
        let storage = self.storage.clone();
        let metadata = self.metadata.clone();

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

            // Look up the item
            let item = {
                let md = metadata.read().await;
                md.items.get(&manifest_req.artifact_id).cloned()
            };
            let item = match item {
                Some(i) => i,
                None => {
                    let _ = tx.send(Err(Status::not_found(
                        format!("Unknown artifact: {}", manifest_req.artifact_id),
                    ))).await;
                    return;
                }
            };

            let root_hash = *item.root();
            let chunk_hashes = storage.chunk_list(&root_hash);

            // Send ManifestResponse
            let resp = SyncMessage {
                msg: Some(Msg::ManifestResponse(proto::ManifestResponse {
                    artifact_id: item.manifest.artifact_id.clone(),
                    version: item.manifest.version,
                    root_hash: root_hash.as_bytes().to_vec(),
                    total_size: item.manifest.total_size,
                    chunk_count: item.manifest.chunk_count,
                    chunk_size: item.manifest.chunk_size,
                    chunk_hashes: chunk_hashes.iter().map(|h| h.as_bytes().to_vec()).collect(),
                    entries: item
                        .manifest
                        .entries
                        .iter()
                        .map(|entry| proto::FileEntry {
                            relative_path: entry.relative_path.clone(),
                            size: entry.size,
                            chunk_start: entry.chunk_range.0,
                            chunk_end: entry.chunk_range.1,
                        })
                        .collect(),
                })),
            };
            if tx.send(Ok(resp)).await.is_err() {
                return;
            }

            // Phase 2: wait for PossessionBitfield
            let bitfield = match in_stream.next().await {
                Some(Ok(SyncMessage { msg: Some(Msg::Possession(poss)) })) => {
                    match Bitfield::from_bytes(&poss.bitfield) {
                        Some(bf) => bf,
                        None => {
                            let _ = tx.send(Err(Status::invalid_argument("Invalid bitfield"))).await;
                            return;
                        }
                    }
                }
                _ => {
                    let _ = tx.send(Err(Status::invalid_argument("Expected PossessionBitfield"))).await;
                    return;
                }
            };

            let missing = bitfield.missing_indices();
            tracing::debug!("Sync: {} missing chunks out of {}", missing.len(), bitfield.chunk_count());

            // Phase 3: stream missing chunks
            // If client has nothing, use BulkData fast path regardless of artifact layout.
            // The client already knows per-chunk sizes from the manifest entries and can split
            // the stream back into individual chunks without per-chunk gRPC framing.
            if bitfield.is_empty() && !missing.is_empty() {
                // Bulk mode: stream concatenated chunks
                const BULK_BATCH: usize = 64;
                for batch_start in (0..missing.len()).step_by(BULK_BATCH) {
                    let batch_end = (batch_start + BULK_BATCH).min(missing.len());
                    let batch_indices = &missing[batch_start..batch_end];

                    let mut bulk_buf = Vec::new();
                    for &idx in batch_indices {
                        if let Some(data) = storage.get_chunk_by_index(&root_hash, idx) {
                            bulk_buf.extend_from_slice(&data);
                        }
                    }

                    let msg = SyncMessage {
                        msg: Some(Msg::BulkData(proto::BulkData {
                            data: bulk_buf,
                            start_index: batch_indices[0],
                            count: batch_indices.len() as u32,
                        })),
                    };
                    if tx.send(Ok(msg)).await.is_err() {
                        return;
                    }
                }
            } else {
                // Per-chunk mode
                for idx in missing {
                    if let Some(data) = storage.get_chunk_by_index(&root_hash, idx) {
                        let msg = SyncMessage {
                            msg: Some(Msg::ChunkData(proto::ChunkData {
                                chunk_index: idx,
                                data,
                            })),
                        };
                        if tx.send(Ok(msg)).await.is_err() {
                            return;
                        }
                    }
                }
            }

            tracing::debug!("Sync complete for {}", item.manifest.artifact_id);
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::SyncStream))
    }
}
