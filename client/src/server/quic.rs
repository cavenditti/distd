use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use distd_core::proto::{sync_message::Msg, ClientKeepAlive, ClientRegister, ManifestRequest, ManifestResponse, PossessionBitfield, ServerMetadata, SyncMessage};
use distd_core::transport::TransportOp;
use distd_core::utils::frame::{read_length_delimited_async, write_length_delimited_async, write_optional_uuid_async, write_transport_op_async};
use quinn::{ClientConfig, Endpoint};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as TlsError, SignatureScheme};
use tokio::io::AsyncRead;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::{ServerConnection, ServerRequest};

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

fn parse_quic_target(url: &str) -> Result<(SocketAddr, String), ServerRequest> {
    let stripped = url
        .strip_prefix("quic://")
        .or_else(|| url.strip_prefix("udp://"))
        .ok_or_else(|| ServerRequest::Connection(ServerConnection::Quic(format!("unsupported QUIC URL: {url}"))))?;
    let addr = stripped
        .to_socket_addrs()
        .map_err(ServerConnection::from)?
        .next()
        .ok_or_else(|| ServerRequest::Connection(ServerConnection::Quic(format!("cannot resolve {stripped}"))))?;
    let server_name = stripped
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(stripped)
        .trim_matches('[')
        .trim_matches(']')
        .to_string();
    let server_name = if server_name.parse::<std::net::IpAddr>().is_ok() {
        String::from("localhost")
    } else {
        server_name
    };
    Ok((addr, server_name))
}

#[derive(Debug)]
pub struct QuicTransportClient {
    _endpoint: Endpoint,
    connection: quinn::Connection,
    client_uuid: Arc<RwLock<Option<Uuid>>>,
}

#[derive(Debug)]
pub struct QuicSyncSession {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

impl QuicTransportClient {
    pub async fn connect(url: &str, client_uuid: Option<Uuid>) -> Result<Self, ServerRequest> {
        let (addr, server_name) = parse_quic_target(url)?;
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|err| ServerRequest::Connection(ServerConnection::Quic(err.to_string())))?;

        let rustls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCertVerifier))
            .with_no_client_auth();
        let client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)
                .map_err(|err| ServerRequest::Connection(ServerConnection::Quic(err.to_string())))?,
        ));
        endpoint.set_default_client_config(client_config);

        let connection = endpoint
            .connect(addr, &server_name)
            .map_err(|err| ServerRequest::Connection(ServerConnection::Quic(err.to_string())))?
            .await
            .map_err(|err| ServerRequest::Connection(ServerConnection::Quic(err.to_string())))?;

        Ok(Self {
            _endpoint: endpoint,
            connection,
            client_uuid: Arc::new(RwLock::new(client_uuid)),
        })
    }

    pub async fn set_client_uuid(&self, client_uuid: Uuid) {
        *self.client_uuid.write().await = Some(client_uuid);
    }

    async fn auth_uuid_for_op(&self, op: TransportOp) -> Result<Option<Uuid>, ServerRequest> {
        match op {
            TransportOp::Register => Ok(*self.client_uuid.read().await),
            TransportOp::Fetch | TransportOp::Sync => self
                .client_uuid
                .read()
                .await
                .ok_or(ServerRequest::MissingUuid)
                .map(Some),
        }
    }

    async fn open_bi(&self, op: TransportOp) -> Result<(quinn::SendStream, quinn::RecvStream), ServerRequest> {
        let (mut send, recv) = self
            .connection
            .open_bi()
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        write_transport_op_async(&mut send, op)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        let auth_uuid = self.auth_uuid_for_op(op).await?;
        write_optional_uuid_async(&mut send, auth_uuid)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        Ok((send, recv))
    }

    pub async fn register(&self, request: ClientRegister) -> Result<ServerMetadata, ServerRequest> {
        let (mut send, mut recv) = self.open_bi(TransportOp::Register).await?;
        write_length_delimited_async(&mut send, &request)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        send.finish().map_err(|err| ServerRequest::Quic(err.to_string()))?;
        read_length_delimited_async(&mut recv)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))
    }

    pub async fn fetch(&self, request: ClientKeepAlive) -> Result<ServerMetadata, ServerRequest> {
        let (mut send, mut recv) = self.open_bi(TransportOp::Fetch).await?;
        write_length_delimited_async(&mut send, &request)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        send.finish().map_err(|err| ServerRequest::Quic(err.to_string()))?;
        read_length_delimited_async(&mut recv)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))
    }

    pub async fn sync(
        &self,
        manifest_request: ManifestRequest,
    ) -> Result<(ManifestResponse, QuicSyncSession), ServerRequest> {
        let (mut send, mut recv) = self.open_bi(TransportOp::Sync).await?;
        write_length_delimited_async(&mut send, &manifest_request)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;

        let manifest = read_sync_manifest_response(&mut recv).await?;

        Ok((manifest, QuicSyncSession { send, recv }))
    }
}

async fn read_sync_manifest_response<R>(reader: &mut R) -> Result<ManifestResponse, ServerRequest>
where
    R: AsyncRead + Unpin,
{
    match read_length_delimited_async::<_, SyncMessage>(reader)
        .await
        .map_err(|err| ServerRequest::Quic(err.to_string()))?
    {
        SyncMessage {
            msg: Some(Msg::ManifestResponse(response)),
        } => Ok(response),
        _ => Err(ServerRequest::UnexpectedMessage),
    }
}

impl QuicSyncSession {
    pub async fn send_possession_and_collect(
        mut self,
        possession: PossessionBitfield,
    ) -> Result<Vec<SyncMessage>, ServerRequest> {
        write_length_delimited_async(&mut self.send, &possession)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        self.send.finish().map_err(|err| ServerRequest::Quic(err.to_string()))?;

        let mut responses = Vec::new();
        loop {
            match read_length_delimited_async::<_, SyncMessage>(&mut self.recv).await {
                Ok(message) => responses.push(message),
                Err(distd_core::utils::frame::FrameError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(ServerRequest::Quic(err.to_string())),
            }
        }

        Ok(responses)
    }
}

#[cfg(test)]
mod tests {
    use super::read_sync_manifest_response;
    use distd_core::proto::{
        sync_message::Msg, ChunkData, ManifestResponse, PayloadCompression, SyncMessage,
    };
    use distd_core::utils::frame::write_length_delimited_async;
    use tokio::io::duplex;

    #[tokio::test]
    async fn reads_manifest_response_from_sync_message_frame() {
        let (mut writer, mut reader) = duplex(4096);
        let expected = ManifestResponse {
            artifact_id: "artifact-a".to_string(),
            version: 7,
            root_hash: vec![1, 2, 3, 4],
            total_size: 42,
            chunk_count: 1,
            chunk_size: 256,
            chunk_hashes: vec![vec![9; 32]],
            entries: Vec::new(),
            chunk_algorithm: None,
            chunk_sizes: vec![42],
        };
        let expected_for_writer = expected.clone();

        tokio::spawn(async move {
            let frame = SyncMessage {
                msg: Some(Msg::ManifestResponse(expected_for_writer)),
            };
            write_length_delimited_async(&mut writer, &frame)
                .await
                .expect("write sync manifest frame");
        });

        let actual = read_sync_manifest_response(&mut reader)
            .await
            .expect("decode manifest response");
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn rejects_non_manifest_sync_message_frame() {
        let (mut writer, mut reader) = duplex(4096);

        tokio::spawn(async move {
            let frame = SyncMessage {
                msg: Some(Msg::ChunkData(ChunkData {
                    chunk_index: 0,
                    data: vec![1, 2, 3],
                    compression: PayloadCompression::Unspecified as i32,
                    uncompressed_size: 3,
                })),
            };
            write_length_delimited_async(&mut writer, &frame)
                .await
                .expect("write non-manifest frame");
        });

        let err = read_sync_manifest_response(&mut reader)
            .await
            .expect_err("non-manifest frame should fail");
        assert!(matches!(err, crate::error::ServerRequest::UnexpectedMessage));
    }
}