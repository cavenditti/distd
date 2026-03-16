use std::fmt::Debug;
use std::net::SocketAddr;

use distd_core::chunk_storage::ChunkStorage;
use distd_core::proto::sync_message::Msg;
use distd_core::proto::{
    ClientKeepAlive, ClientRegister, ManifestRequest, PossessionBitfield, SyncMessage,
};
use distd_core::transport::TransportOp;
use distd_core::utils::frame::{
    read_length_delimited_async, read_optional_uuid_async, read_transport_op_async,
    write_length_delimited_async,
};
use quinn::{Endpoint, RecvStream, SendStream};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use crate::error::Server as ServerError;
use crate::Server;

fn make_server_config() -> Result<quinn::ServerConfig, ServerError> {
    let certified =
        generate_simple_self_signed(vec![String::from("localhost"), String::from("127.0.0.1")])
            .map_err(|err| ServerError::Quic(err.to_string()))?;
    let cert_der = CertificateDer::from(certified.cert.der().clone());
    let key_der = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()));
    quinn::ServerConfig::with_single_cert(vec![cert_der], key_der)
        .map_err(|err| ServerError::Quic(err.to_string()))
}

async fn finish_send(send: &mut SendStream) -> Result<(), ServerError> {
    send.finish()
        .map_err(|err| ServerError::Quic(err.to_string()))?;
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
                        let (manifest_response, item) = self
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
                            .sync_chunk_response_messages(&item, possession)
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

#[cfg(test)]
mod tests {
    use super::make_server_config;
    use crate::Server;
    use distd_core::chunk_storage::hashmap_storage::HashMapStorage;
    use distd_core::metadata::Server as ServerMetadata;
    use distd_core::possession::Bitfield;
    use distd_core::proto::{
        sync_message::Msg, ClientKeepAlive, ClientRegister, ManifestRequest, PayloadCompression,
        PossessionBitfield, SyncMessage,
    };
    use distd_core::transport::TransportOp;
    use distd_core::utils::frame::{
        read_length_delimited_async, write_length_delimited_async, write_optional_uuid_async,
        write_transport_op_async,
    };
    use quinn::{ClientConfig, Endpoint};
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error as TlsError, SignatureScheme};
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;
    use uuid::Uuid;

    #[derive(Debug)]
    struct AcceptAnyServerCertVerifier;

    impl ServerCertVerifier for AcceptAnyServerCertVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, TlsError> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, TlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, TlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ED25519,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
            ]
        }
    }

    async fn connect_client(addr: SocketAddr) -> (Endpoint, quinn::Connection) {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap()).expect("client endpoint");
        let rustls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCertVerifier))
            .with_no_client_auth();
        let client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)
                .expect("quic client config"),
        ));
        endpoint.set_default_client_config(client_config);

        for _ in 0..20 {
            match endpoint.connect(addr, "localhost") {
                Ok(connecting) => match connecting.await {
                    Ok(connection) => return (endpoint, connection),
                    Err(_) => sleep(Duration::from_millis(25)).await,
                },
                Err(_) => sleep(Duration::from_millis(25)).await,
            }
        }

        panic!("failed to connect QUIC client to {addr}");
    }

    async fn open_authed_stream(
        connection: &quinn::Connection,
        op: TransportOp,
        uuid: Option<Uuid>,
    ) -> (quinn::SendStream, quinn::RecvStream) {
        let (mut send, recv) = connection.open_bi().await.expect("open bi");
        write_transport_op_async(&mut send, op)
            .await
            .expect("write transport op");
        write_optional_uuid_async(&mut send, uuid)
            .await
            .expect("write auth uuid");
        (send, recv)
    }

    async fn start_test_server() -> (
        tokio::task::JoinHandle<()>,
        SocketAddr,
        Server<HashMapStorage>,
    ) {
        let server = Server::new_ephemeral(HashMapStorage::default());
        let endpoint = Endpoint::server(
            make_server_config().expect("server config"),
            "127.0.0.1:0".parse().unwrap(),
        )
        .expect("server endpoint");
        let addr = endpoint.local_addr().expect("local addr");
        let task_server = server.clone();
        let handle = tokio::spawn(async move {
            while let Some(connecting) = endpoint.accept().await {
                let server = task_server.clone();
                tokio::spawn(async move {
                    let connection = match connecting.await {
                        Ok(connection) => connection,
                        Err(_) => return,
                    };

                    loop {
                        let remote_addr = connection.remote_address();
                        let (mut send, mut recv) = match connection.accept_bi().await {
                            Ok(streams) => streams,
                            Err(_) => return,
                        };
                        let server = server.clone();
                        tokio::spawn(async move {
                            let _ = server
                                .handle_quic_stream(remote_addr, &mut send, &mut recv)
                                .await;
                        });
                    }
                });
            }
        });

        (handle, addr, server)
    }

    async fn register_client_uuid(connection: &quinn::Connection) -> Uuid {
        let (mut send, mut recv) =
            open_authed_stream(connection, TransportOp::Register, None).await;
        write_length_delimited_async(
            &mut send,
            &ClientRegister {
                name: "test-client".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                uuid: None,
            },
        )
        .await
        .expect("write register request");
        send.finish().expect("finish register");
        let register =
            read_length_delimited_async::<_, distd_core::proto::ServerMetadata>(&mut recv)
                .await
                .expect("read register response");
        Uuid::from_slice(register.uuid.as_ref().expect("uuid present")).expect("valid uuid")
    }

    #[tokio::test]
    async fn quic_register_and_fetch_round_trip() {
        let (server_task, addr, server) = start_test_server().await;
        server
            .publish_item(
                "artifact-a".to_string(),
                "artifact-a".into(),
                Some("test artifact".to_string()),
                axum::body::Bytes::from_static(b"hello over quic"),
                distd_core::chunks::ChunkAlgorithm::default(),
            )
            .await
            .expect("publish test item");

        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;

        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Fetch, Some(uuid)).await;
        write_length_delimited_async(&mut send, &ClientKeepAlive {})
            .await
            .expect("write fetch request");
        send.finish().expect("finish fetch");

        let fetch = read_length_delimited_async::<_, distd_core::proto::ServerMetadata>(&mut recv)
            .await
            .expect("read fetch response");
        let decoded: ServerMetadata =
            bitcode::deserialize(&fetch.serialized).expect("decode metadata");
        assert!(decoded.items.contains_key("artifact-a"));

        server_task.abort();
    }

    #[tokio::test]
    async fn quic_sync_round_trip_returns_manifest_and_payload() {
        let (server_task, addr, server) = start_test_server().await;
        let item = server
            .publish_item(
                "artifact-a".to_string(),
                "artifact-a".into(),
                Some("test artifact".to_string()),
                axum::body::Bytes::from_static(b"hello over quic"),
                distd_core::chunks::ChunkAlgorithm::default(),
            )
            .await
            .expect("publish test item");

        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;

        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Sync, Some(uuid)).await;
        write_length_delimited_async(
            &mut send,
            &ManifestRequest {
                artifact_id: "artifact-a".to_string(),
                version: None,
            },
        )
        .await
        .expect("write manifest request");

        let manifest = read_length_delimited_async::<_, SyncMessage>(&mut recv)
            .await
            .expect("read manifest frame");
        let manifest = match manifest.msg {
            Some(Msg::ManifestResponse(manifest)) => manifest,
            other => panic!("unexpected manifest response: {other:?}"),
        };
        assert_eq!(manifest.artifact_id, "artifact-a");
        assert_eq!(manifest.chunk_count as usize, item.chunks.len());

        write_length_delimited_async(
            &mut send,
            &PossessionBitfield {
                bitfield: Bitfield::empty(item.manifest.chunk_count).to_bytes(),
            },
        )
        .await
        .expect("write possession bitfield");
        send.finish().expect("finish sync request");

        let mut saw_payload = false;
        loop {
            match read_length_delimited_async::<_, SyncMessage>(&mut recv).await {
                Ok(SyncMessage {
                    msg: Some(Msg::ChunkData(chunk)),
                }) => {
                    assert_eq!(chunk.compression, PayloadCompression::Unspecified as i32);
                    assert!(!chunk.data.is_empty());
                    saw_payload = true;
                }
                Ok(SyncMessage {
                    msg: Some(Msg::BulkData(bulk)),
                }) => {
                    assert_eq!(bulk.compression, PayloadCompression::Unspecified as i32);
                    assert!(!bulk.data.is_empty());
                    saw_payload = true;
                }
                Ok(other) => panic!("unexpected sync frame: {other:?}"),
                Err(distd_core::utils::frame::FrameError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break
                }
                Err(err) => panic!("unexpected sync error: {err}"),
            }
        }
        assert!(saw_payload, "expected at least one payload frame");

        server_task.abort();
    }

    #[tokio::test]
    async fn quic_fetch_rejects_unauthenticated_client() {
        let (server_task, addr, _server) = start_test_server().await;
        let (_endpoint, connection) = connect_client(addr).await;

        let (mut send, mut recv) = open_authed_stream(&connection, TransportOp::Fetch, None).await;
        write_length_delimited_async(&mut send, &ClientKeepAlive {})
            .await
            .expect("write fetch request");
        send.finish().expect("finish fetch");

        match read_length_delimited_async::<_, distd_core::proto::ServerMetadata>(&mut recv).await {
            Err(distd_core::utils::frame::FrameError::IoError(err))
                if err.kind() == std::io::ErrorKind::UnexpectedEof => {}
            other => panic!("expected EOF for unauthenticated fetch, got {other:?}"),
        }

        server_task.abort();
    }

    #[tokio::test]
    async fn quic_sync_rejects_invalid_possession_bitfield() {
        let (server_task, addr, server) = start_test_server().await;
        server
            .publish_item(
                "artifact-a".to_string(),
                "artifact-a".into(),
                Some("test artifact".to_string()),
                axum::body::Bytes::from_static(b"hello over quic"),
                distd_core::chunks::ChunkAlgorithm::default(),
            )
            .await
            .expect("publish test item");

        let (_endpoint, connection) = connect_client(addr).await;
        let uuid = register_client_uuid(&connection).await;

        let (mut send, mut recv) =
            open_authed_stream(&connection, TransportOp::Sync, Some(uuid)).await;
        write_length_delimited_async(
            &mut send,
            &ManifestRequest {
                artifact_id: "artifact-a".to_string(),
                version: None,
            },
        )
        .await
        .expect("write manifest request");

        let manifest = read_length_delimited_async::<_, SyncMessage>(&mut recv)
            .await
            .expect("read manifest frame");
        match manifest.msg {
            Some(Msg::ManifestResponse(_)) => {}
            other => panic!("unexpected manifest response: {other:?}"),
        }

        write_length_delimited_async(
            &mut send,
            &PossessionBitfield {
                bitfield: vec![0, 0, 0, 0],
            },
        )
        .await
        .expect("write malformed possession bitfield");
        send.finish().expect("finish sync request");

        match read_length_delimited_async::<_, SyncMessage>(&mut recv).await {
            Err(distd_core::utils::frame::FrameError::IoError(err))
                if err.kind() == std::io::ErrorKind::UnexpectedEof => {}
            other => panic!("expected EOF after invalid bitfield, got {other:?}"),
        }

        server_task.abort();
    }
}
