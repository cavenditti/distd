use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};

use distd_core::proto::{
    sync_message::Msg, ClientKeepAlive, ClientRegister, ManifestRequest, ManifestResponse,
    PossessionBitfield, ServerMetadata, SyncMessage,
};
use distd_core::transport::{decode_sync_payload_owned, TransportOp};
use distd_core::utils::frame::{
    read_length_delimited_async, write_length_delimited_async, write_optional_uuid_async,
    write_transport_op_async,
};
use quinn::{ClientConfig, Endpoint};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, Error as TlsError, SignatureScheme};
use tokio::io::AsyncRead;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::error::{ServerConnection, ServerRequest};

#[derive(Debug)]
struct PinnedServerCertVerifier {
    expected: Option<[u8; 32]>,
    observed: Mutex<Option<[u8; 32]>>,
    supported: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl PinnedServerCertVerifier {
    fn new(expected: Option<[u8; 32]>) -> Self {
        Self {
            expected,
            observed: Mutex::new(None),
            supported: rustls::crypto::ring::default_provider().signature_verification_algorithms,
        }
    }

    fn observed(&self) -> Option<[u8; 32]> {
        *self.observed.lock().unwrap()
    }
}

impl ServerCertVerifier for PinnedServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        let fingerprint = *blake3::hash(end_entity.as_ref()).as_bytes();
        if self
            .expected
            .is_some_and(|expected| expected != fingerprint)
        {
            return Err(TlsError::General(
                "server certificate does not match the pinned identity".to_string(),
            ));
        }
        *self.observed.lock().unwrap() = Some(fingerprint);
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported.supported_schemes()
    }
}

fn parse_quic_target(url: &str) -> Result<(SocketAddr, String), ServerRequest> {
    let stripped = url
        .strip_prefix("quic://")
        .or_else(|| url.strip_prefix("udp://"))
        .ok_or_else(|| {
            ServerRequest::Connection(ServerConnection::Quic(format!(
                "unsupported QUIC URL: {url}"
            )))
        })?;
    let addresses = stripped
        .to_socket_addrs()
        .map_err(ServerConnection::from)?
        .collect::<Vec<_>>();
    let addr = addresses
        .iter()
        .copied()
        .find(SocketAddr::is_ipv4)
        .or_else(|| addresses.first().copied())
        .ok_or_else(|| {
            ServerRequest::Connection(ServerConnection::Quic(format!("cannot resolve {stripped}")))
        })?;
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
    certificate_fingerprint: [u8; 32],
}

#[derive(Debug)]
pub struct QuicSyncSession {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    connection: quinn::Connection,
    starting_received_bytes: u64,
}

impl QuicTransportClient {
    pub async fn connect(
        url: &str,
        client_uuid: Option<Uuid>,
        expected_certificate: Option<[u8; 32]>,
    ) -> Result<Self, ServerRequest> {
        let (addr, server_name) = parse_quic_target(url)?;
        let bind_addr = if addr.is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };
        let mut endpoint = Endpoint::client(bind_addr.parse().unwrap())
            .map_err(|err| ServerRequest::Connection(ServerConnection::Quic(err.to_string())))?;

        let verifier = Arc::new(PinnedServerCertVerifier::new(expected_certificate));
        let rustls_config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier.clone())
            .with_no_client_auth();
        let mut client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config).map_err(|err| {
                ServerRequest::Connection(ServerConnection::Quic(err.to_string()))
            })?,
        ));

        let mut transport = quinn::TransportConfig::default();
        transport.receive_window(quinn::VarInt::from_u32(64 * 1024 * 1024));
        transport.stream_receive_window(quinn::VarInt::from_u32(32 * 1024 * 1024));
        transport.send_window(64 * 1024 * 1024);
        transport.keep_alive_interval(Some(std::time::Duration::from_secs(5)));
        transport.max_concurrent_bidi_streams(quinn::VarInt::from_u32(32));
        transport.max_concurrent_uni_streams(quinn::VarInt::from_u32(0));
        transport.initial_rtt(std::time::Duration::from_millis(1));
        let mut bbr = quinn::congestion::BbrConfig::default();
        bbr.initial_window(1024 * 1024);
        transport.congestion_controller_factory(std::sync::Arc::new(bbr));

        client_config.transport_config(std::sync::Arc::new(transport));

        endpoint.set_default_client_config(client_config);

        let connection = endpoint
            .connect(addr, &server_name)
            .map_err(|err| ServerRequest::Connection(ServerConnection::Quic(err.to_string())))?
            .await
            .map_err(|err| ServerRequest::Connection(ServerConnection::Quic(err.to_string())))?;
        let certificate_fingerprint = verifier.observed().ok_or_else(|| {
            ServerRequest::Connection(ServerConnection::Quic(
                "server did not present a certificate".to_string(),
            ))
        })?;

        Ok(Self {
            _endpoint: endpoint,
            connection,
            client_uuid: Arc::new(RwLock::new(client_uuid)),
            certificate_fingerprint,
        })
    }

    pub fn certificate_fingerprint(&self) -> [u8; 32] {
        self.certificate_fingerprint
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

    async fn open_bi(
        &self,
        op: TransportOp,
    ) -> Result<(quinn::SendStream, quinn::RecvStream), ServerRequest> {
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
        send.finish()
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        read_length_delimited_async(&mut recv)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))
    }

    pub async fn fetch(&self, request: ClientKeepAlive) -> Result<ServerMetadata, ServerRequest> {
        let (mut send, mut recv) = self.open_bi(TransportOp::Fetch).await?;
        write_length_delimited_async(&mut send, &request)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        send.finish()
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        read_length_delimited_async(&mut recv)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))
    }

    pub async fn sync(
        &self,
        manifest_request: ManifestRequest,
    ) -> Result<(ManifestResponse, QuicSyncSession), ServerRequest> {
        let starting_received_bytes = self.connection.stats().udp_rx.bytes;
        let (mut send, mut recv) = self.open_bi(TransportOp::Sync).await?;
        write_length_delimited_async(&mut send, &manifest_request)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;

        let manifest = read_sync_manifest_response(&mut recv).await?;

        Ok((
            manifest,
            QuicSyncSession {
                send,
                recv,
                connection: self.connection.clone(),
                starting_received_bytes,
            },
        ))
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
    pub async fn send_possession_and_collect_chunks(
        mut self,
        possession: PossessionBitfield,
        chunk_sizes: &[usize],
    ) -> Result<(Vec<Vec<u8>>, u64), ServerRequest> {
        write_length_delimited_async(&mut self.send, &possession)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        self.send
            .finish()
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;

        let mut received = Vec::new();
        loop {
            match read_length_delimited_async::<_, SyncMessage>(&mut self.recv).await {
                Ok(message) => decode_sync_message_into(message, chunk_sizes, &mut received)?,
                Err(distd_core::utils::frame::FrameError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break
                }
                Err(err) => return Err(ServerRequest::Quic(err.to_string())),
            }
        }

        let wire_bytes = self
            .connection
            .stats()
            .udp_rx
            .bytes
            .saturating_sub(self.starting_received_bytes);
        Ok((received, wire_bytes))
    }
}

fn decode_sync_message_into(
    message: SyncMessage,
    chunk_sizes: &[usize],
    received: &mut Vec<Vec<u8>>,
) -> Result<(), ServerRequest> {
    match message {
        SyncMessage {
            msg: Some(Msg::ChunkData(cd)),
        } => {
            received.push(decode_sync_payload_owned(
                cd.compression,
                cd.data,
                cd.uncompressed_size,
            )?);
            Ok(())
        }
        SyncMessage {
            msg: Some(Msg::BulkData(bd)),
        } => {
            let decoded = decode_sync_payload_owned(bd.compression, bd.data, bd.uncompressed_size)?;
            let mut offset = 0;
            for i in 0..bd.count {
                let idx = bd.start_index + i;
                let this_size = *chunk_sizes
                    .get(idx as usize)
                    .ok_or(ServerRequest::UnexpectedMessage)?;
                if offset + this_size > decoded.len() {
                    return Err(ServerRequest::UnexpectedMessage);
                }
                received.push(decoded[offset..offset + this_size].to_vec());
                offset += this_size;
            }

            if offset != decoded.len() {
                return Err(ServerRequest::UnexpectedMessage);
            }

            Ok(())
        }
        _ => Err(ServerRequest::UnexpectedMessage),
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_sync_message_into, parse_quic_target, read_sync_manifest_response};
    use distd_core::proto::{
        sync_message::Msg, BulkData, ChunkData, ManifestResponse, PayloadCompression, SyncMessage,
    };
    use distd_core::utils::frame::write_length_delimited_async;
    use tokio::io::duplex;

    #[test]
    fn localhost_prefers_an_ipv4_target_for_the_ipv4_server_listener() {
        let (address, server_name) =
            parse_quic_target("quic://localhost:50051").expect("resolve localhost");

        assert!(address.is_ipv4());
        assert_eq!(server_name, "localhost");
    }

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
        assert!(matches!(
            err,
            crate::error::ServerRequest::UnexpectedMessage
        ));
    }

    #[test]
    fn decodes_bulk_data_frames_directly_into_chunks() {
        let mut received = Vec::new();
        let message = SyncMessage {
            msg: Some(Msg::BulkData(BulkData {
                start_index: 1,
                count: 2,
                data: vec![3, 4, 5, 6, 7],
                compression: PayloadCompression::Unspecified as i32,
                uncompressed_size: 5,
            })),
        };

        decode_sync_message_into(message, &[2, 2, 3], &mut received).expect("decode bulk frame");

        assert_eq!(received, vec![vec![3, 4], vec![5, 6, 7]]);
    }

    #[test]
    fn rejects_bulk_data_with_invalid_decoded_length() {
        let mut received = Vec::new();
        let message = SyncMessage {
            msg: Some(Msg::BulkData(BulkData {
                start_index: 0,
                count: 2,
                data: vec![1, 2, 3],
                compression: PayloadCompression::Unspecified as i32,
                uncompressed_size: 3,
            })),
        };

        let err = decode_sync_message_into(message, &[2, 2], &mut received)
            .expect_err("short bulk frame should fail");
        assert!(matches!(
            err,
            crate::error::ServerRequest::UnexpectedMessage
        ));
    }
}
