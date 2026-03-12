use std::fmt::Debug;
use std::net::SocketAddr;

use distd_core::chunk_storage::ChunkStorage;
use distd_core::proto::{ClientKeepAlive, ClientRegister, ManifestRequest, PossessionBitfield, SyncMessage};
use distd_core::proto::sync_message::Msg;
use distd_core::transport::TransportOp;
use distd_core::utils::frame::{read_length_delimited_async, read_optional_uuid_async, read_transport_op_async, write_length_delimited_async};
use quinn::{Endpoint, RecvStream, SendStream};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::error::Server as ServerError;
use crate::Server;

fn make_server_config() -> Result<quinn::ServerConfig, ServerError> {
    let certified = generate_simple_self_signed(vec![
        String::from("localhost"),
        String::from("127.0.0.1"),
    ])
    .map_err(|err| ServerError::Quic(err.to_string()))?;
    let cert_der = CertificateDer::from(certified.cert.der().clone());
    let key_der = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()));
    quinn::ServerConfig::with_single_cert(vec![cert_der], key_der)
        .map_err(|err| ServerError::Quic(err.to_string()))
}

async fn finish_send(send: &mut SendStream) -> Result<(), ServerError> {
    send.finish().map_err(|err| ServerError::Quic(err.to_string()))?;
    Ok(())
}

impl<T> Server<T>
where
    T: ChunkStorage + Sync + Send + Debug + 'static,
{
    async fn handle_quic_stream(
        &self,
        remote_addr: SocketAddr,
        send: &mut SendStream,
        recv: &mut RecvStream,
    ) -> Result<(), ServerError> {
        match read_transport_op_async(recv).await {
            Ok(op) => {
                let client_uuid = read_optional_uuid_async(recv)
                    .await
                    .map_err(|err| ServerError::Quic(err.to_string()))?;
                match op {
                    TransportOp::Register => {
                let request: ClientRegister = read_length_delimited_async(recv)
                    .await
                    .map_err(|err| ServerError::Quic(err.to_string()))?;
                if client_uuid.is_some() && request.uuid.is_none() {
                    tracing::debug!("QUIC register stream carried client auth UUID");
                }
                let response = self
                    .register_response(remote_addr, request)
                    .await
                    .map_err(ServerError::Quic)?;
                write_length_delimited_async(send, &response)
                    .await
                    .map_err(|err| ServerError::Quic(err.to_string()))?;
                finish_send(send).await
                    }
                    TransportOp::Fetch => {
                self.authenticate_client_uuid(client_uuid)
                    .await
                    .map_err(ServerError::Quic)?;
                let request: ClientKeepAlive = read_length_delimited_async(recv)
                    .await
                    .map_err(|err| ServerError::Quic(err.to_string()))?;
                let response = self
                    .fetch_response(request)
                    .await
                    .map_err(ServerError::Quic)?;
                write_length_delimited_async(send, &response)
                    .await
                    .map_err(|err| ServerError::Quic(err.to_string()))?;
                finish_send(send).await
                    }
                    TransportOp::Sync => {
                self.authenticate_client_uuid(client_uuid)
                    .await
                    .map_err(ServerError::Quic)?;
                let manifest_request: ManifestRequest = read_length_delimited_async(recv)
                    .await
                    .map_err(|err| ServerError::Quic(err.to_string()))?;
                let (manifest_response, root_hash, expected_chunk_count) = self
                    .sync_manifest_response(manifest_request)
                    .await
                    .map_err(ServerError::Quic)?;
                write_length_delimited_async(
                    send,
                    &SyncMessage {
                        msg: Some(Msg::ManifestResponse(manifest_response)),
                    },
                )
                .await
                .map_err(|err| ServerError::Quic(err.to_string()))?;
                let possession: PossessionBitfield = read_length_delimited_async(recv)
                    .await
                    .map_err(|err| ServerError::Quic(err.to_string()))?;
                let responses = self
                    .sync_chunk_response_messages(root_hash, expected_chunk_count, possession)
                    .await
                    .map_err(ServerError::Quic)?;
                for response in responses {
                    write_length_delimited_async(send, &response)
                        .await
                        .map_err(|err| ServerError::Quic(err.to_string()))?;
                }
                finish_send(send).await
                    }
                }
            }
            Err(err) => Err(ServerError::Quic(err.to_string())),
        }
    }

    pub async fn serve_quic(self, bind_addr: SocketAddr) -> Result<(), ServerError> {
        let endpoint = Endpoint::server(make_server_config()?, bind_addr)
            .map_err(|err| ServerError::Quic(err.to_string()))?;

        tracing::info!("listening on {} for QUIC", bind_addr);

        while let Some(connecting) = endpoint.accept().await {
            let server = self.clone();
            tokio::spawn(async move {
                let connection = match connecting.await {
                    Ok(connection) => connection,
                    Err(err) => {
                        tracing::warn!("QUIC connection failed: {err}");
                        return;
                    }
                };

                loop {
                    let remote_addr = connection.remote_address();
                    let (mut send, mut recv) = match connection.accept_bi().await {
                        Ok(streams) => streams,
                        Err(err) => {
                            tracing::debug!("QUIC connection closed: {err}");
                            return;
                        }
                    };
                    let server = server.clone();
                    tokio::spawn(async move {
                        if let Err(err) = server
                            .handle_quic_stream(remote_addr, &mut send, &mut recv)
                            .await
                        {
                            tracing::warn!("QUIC stream handling failed: {err}");
                        }
                    });
                }
            });
        }

        Ok(())
    }
}