use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};

use distd_core::chunk_storage::ReceivedChunk;
use distd_core::possession::Bitfield;
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

use crate::{
    error::{ServerConnection, ServerRequest},
    server_url::CanonicalServerUrl,
};

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

fn resolve_quic_target(url: &CanonicalServerUrl) -> Result<(SocketAddr, String), ServerRequest> {
    let addresses = url
        .dial_authority()
        .to_socket_addrs()
        .map_err(ServerConnection::from)?
        .collect::<Vec<_>>();
    let addr = addresses
        .iter()
        .copied()
        .find(SocketAddr::is_ipv4)
        .or_else(|| addresses.first().copied())
        .ok_or_else(|| {
            ServerRequest::Connection(ServerConnection::Quic(
                "canonical server authority resolved to no addresses".to_string(),
            ))
        })?;
    let server_name = if url.tls_server_name().parse::<std::net::IpAddr>().is_ok() {
        String::from("localhost")
    } else {
        url.tls_server_name().to_owned()
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
        url: &CanonicalServerUrl,
        client_uuid: Option<Uuid>,
        expected_certificate: Option<[u8; 32]>,
    ) -> Result<Self, ServerRequest> {
        let (addr, server_name) = resolve_quic_target(url)?;
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

    pub fn close(&self) {
        self.connection
            .close(quinn::VarInt::from_u32(0), b"enrollment not activated");
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
    #[allow(dead_code)]
    pub async fn send_possession_and_collect_chunks(
        self,
        possession: PossessionBitfield,
        chunk_sizes: &[usize],
    ) -> Result<(Vec<Vec<u8>>, u64), ServerRequest> {
        let (received, wire_bytes) = self
            .send_possession_and_collect_chunks_shared(possession, chunk_sizes)
            .await?;
        Ok((
            received.into_iter().map(ReceivedChunk::into_vec).collect(),
            wire_bytes,
        ))
    }

    pub async fn send_possession_and_collect_chunks_shared(
        mut self,
        possession: PossessionBitfield,
        chunk_sizes: &[usize],
    ) -> Result<(Vec<ReceivedChunk>, u64), ServerRequest> {
        let bitfield = Bitfield::from_bytes(&possession.bitfield)
            .filter(|bitfield| bitfield.chunk_count() as usize == chunk_sizes.len())
            .ok_or(ServerRequest::UnexpectedMessage)?;
        let expected_missing = bitfield.missing_indices();

        write_length_delimited_async(&mut self.send, &possession)
            .await
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;
        self.send
            .finish()
            .map_err(|err| ServerRequest::Quic(err.to_string()))?;

        let mut decoder = SyncChunkDecoder::new(chunk_sizes, expected_missing);
        loop {
            match read_length_delimited_async::<_, SyncMessage>(&mut self.recv).await {
                Ok(message) => decoder.push(message)?,
                Err(distd_core::utils::frame::FrameError::IoError(err))
                    if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break
                }
                Err(err) => return Err(ServerRequest::Quic(err.to_string())),
            }
        }
        let received = decoder.finish()?;

        let wire_bytes = self
            .connection
            .stats()
            .udp_rx
            .bytes
            .saturating_sub(self.starting_received_bytes);
        Ok((received, wire_bytes))
    }
}

struct SyncChunkDecoder<'a> {
    chunk_sizes: &'a [usize],
    expected_missing: Vec<u32>,
    next_expected: usize,
    received: Vec<ReceivedChunk>,
}

impl<'a> SyncChunkDecoder<'a> {
    fn new(chunk_sizes: &'a [usize], expected_missing: Vec<u32>) -> Self {
        Self {
            chunk_sizes,
            expected_missing,
            next_expected: 0,
            received: Vec::new(),
        }
    }

    fn push(&mut self, message: SyncMessage) -> Result<(), ServerRequest> {
        match message {
            SyncMessage {
                msg: Some(Msg::ChunkData(cd)),
            } => {
                if self.expected_missing.get(self.next_expected) != Some(&cd.chunk_index) {
                    return Err(ServerRequest::UnexpectedMessage);
                }
                let decoded = ReceivedChunk::from(decode_sync_payload_owned(
                    cd.compression,
                    cd.data,
                    cd.uncompressed_size,
                )?);
                let expected_size = *self
                    .chunk_sizes
                    .get(cd.chunk_index as usize)
                    .ok_or(ServerRequest::UnexpectedMessage)?;
                if decoded.len() != expected_size {
                    return Err(ServerRequest::UnexpectedMessage);
                }
                self.received.push(decoded);
                self.next_expected += 1;
                Ok(())
            }
            SyncMessage {
                msg: Some(Msg::BulkData(bd)),
            } => {
                let count =
                    usize::try_from(bd.count).map_err(|_| ServerRequest::UnexpectedMessage)?;
                if count == 0 {
                    return Err(ServerRequest::UnexpectedMessage);
                }
                let expected_end = self
                    .next_expected
                    .checked_add(count)
                    .ok_or(ServerRequest::UnexpectedMessage)?;
                let expected = self
                    .expected_missing
                    .get(self.next_expected..expected_end)
                    .ok_or(ServerRequest::UnexpectedMessage)?;
                if expected.first() != Some(&bd.start_index)
                    || !expected
                        .windows(2)
                        .all(|pair| pair[0].checked_add(1) == Some(pair[1]))
                {
                    return Err(ServerRequest::UnexpectedMessage);
                }

                let end_index = bd
                    .start_index
                    .checked_add(bd.count)
                    .ok_or(ServerRequest::UnexpectedMessage)?;
                let start_index = usize::try_from(bd.start_index)
                    .map_err(|_| ServerRequest::UnexpectedMessage)?;
                let end_index =
                    usize::try_from(end_index).map_err(|_| ServerRequest::UnexpectedMessage)?;
                let selected_sizes = self
                    .chunk_sizes
                    .get(start_index..end_index)
                    .ok_or(ServerRequest::UnexpectedMessage)?;
                let decoded = ReceivedChunk::from(decode_sync_payload_owned(
                    bd.compression,
                    bd.data,
                    bd.uncompressed_size,
                )?);

                let mut offset = 0usize;
                let mut ranges = Vec::with_capacity(selected_sizes.len());
                for &this_size in selected_sizes {
                    let end = offset
                        .checked_add(this_size)
                        .ok_or(ServerRequest::UnexpectedMessage)?;
                    if end > decoded.len() {
                        return Err(ServerRequest::UnexpectedMessage);
                    }
                    ranges.push(offset..end);
                    offset = end;
                }

                if offset != decoded.len() {
                    return Err(ServerRequest::UnexpectedMessage);
                }

                self.received
                    .extend(ranges.into_iter().map(|range| decoded.slice(range)));
                self.next_expected = expected_end;
                Ok(())
            }
            _ => Err(ServerRequest::UnexpectedMessage),
        }
    }

    fn finish(self) -> Result<Vec<ReceivedChunk>, ServerRequest> {
        if self.next_expected != self.expected_missing.len() {
            return Err(ServerRequest::UnexpectedMessage);
        }
        Ok(self.received)
    }
}

#[cfg(test)]
mod tests {
    use super::{read_sync_manifest_response, resolve_quic_target, SyncChunkDecoder};
    use crate::server_url::CanonicalServerUrl;
    use distd_core::proto::{
        sync_message::Msg, BulkData, ChunkData, ManifestResponse, PayloadCompression, SyncMessage,
    };
    use distd_core::transport::maybe_compress_sync_payload_owned;
    use distd_core::utils::frame::write_length_delimited_async;
    use tokio::io::duplex;

    #[test]
    fn localhost_prefers_an_ipv4_target_for_the_ipv4_server_listener() {
        let canonical = CanonicalServerUrl::parse("quic://localhost:50051").expect("canonical URL");
        let (address, server_name) = resolve_quic_target(&canonical).expect("resolve localhost");

        assert!(address.is_ipv4());
        assert_eq!(server_name, "localhost");
    }

    #[test]
    fn canonical_authority_drives_dial_and_tls_names() {
        let canonical = CanonicalServerUrl::parse("udp://LOCALHOST:50051").expect("canonical URL");
        assert_eq!(canonical.as_str(), "quic://localhost:50051");

        let (_, server_name) = resolve_quic_target(&canonical).expect("resolve localhost");
        assert_eq!(server_name, canonical.tls_server_name());
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
            publisher_public_key: Vec::new(),
            publisher_signature: Vec::new(),
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
        let mut decoder = SyncChunkDecoder::new(&[2, 2, 3], vec![1, 2]);
        let message = SyncMessage {
            msg: Some(Msg::BulkData(BulkData {
                start_index: 1,
                count: 2,
                data: vec![3, 4, 5, 6, 7],
                compression: PayloadCompression::Unspecified as i32,
                uncompressed_size: 5,
            })),
        };

        decoder.push(message).expect("decode bulk frame");
        let received = decoder.finish().expect("all chunks consumed");

        assert_eq!(received[0].as_ref(), [3, 4]);
        assert_eq!(received[1].as_ref(), [5, 6, 7]);
        assert_eq!(
            received[1].as_ptr(),
            received[0].as_ptr().wrapping_add(received[0].len()),
            "bulk chunks must be adjacent views into one decoded allocation"
        );
    }

    #[test]
    fn rejects_bulk_data_with_invalid_decoded_length() {
        let mut decoder = SyncChunkDecoder::new(&[2, 2], vec![0, 1]);
        let message = SyncMessage {
            msg: Some(Msg::BulkData(BulkData {
                start_index: 0,
                count: 2,
                data: vec![1, 2, 3],
                compression: PayloadCompression::Unspecified as i32,
                uncompressed_size: 3,
            })),
        };

        let err = decoder
            .push(message)
            .expect_err("short bulk frame should fail");
        assert!(matches!(
            err,
            crate::error::ServerRequest::UnexpectedMessage
        ));
        assert!(decoder.received.is_empty(), "a malformed frame is atomic");
    }

    #[test]
    fn rejects_bulk_data_with_invalid_index_range_without_appending() {
        let mut decoder = SyncChunkDecoder::new(&[2, 2], vec![0, 1]);
        let message = SyncMessage {
            msg: Some(Msg::BulkData(BulkData {
                start_index: u32::MAX,
                count: 2,
                data: Vec::new(),
                compression: PayloadCompression::Unspecified as i32,
                uncompressed_size: 0,
            })),
        };

        let err = decoder
            .push(message)
            .expect_err("overflowing bulk range should fail");

        assert!(matches!(
            err,
            crate::error::ServerRequest::UnexpectedMessage
        ));
        assert!(decoder.received.is_empty(), "a malformed frame is atomic");
    }

    #[test]
    fn bulk_chunk_slices_own_the_decoded_payload_lifetime() {
        let mut decoder = SyncChunkDecoder::new(&[2, 2], vec![0, 1]);
        decoder
            .push(SyncMessage {
                msg: Some(Msg::BulkData(BulkData {
                    start_index: 0,
                    count: 2,
                    data: vec![10, 11, 12, 13],
                    compression: PayloadCompression::Unspecified as i32,
                    uncompressed_size: 4,
                })),
            })
            .expect("decode bulk frame");
        let received = decoder.finish().expect("all chunks consumed");

        let retained = received[1].clone();
        drop(received);

        assert_eq!(retained.as_ref(), [12, 13]);
    }

    #[test]
    fn mixed_chunk_and_bulk_messages_preserve_wire_order() {
        let mut decoder = SyncChunkDecoder::new(&[2, 2, 3], vec![0, 1, 2]);
        decoder
            .push(SyncMessage {
                msg: Some(Msg::ChunkData(ChunkData {
                    chunk_index: 0,
                    data: vec![1, 2],
                    compression: PayloadCompression::Unspecified as i32,
                    uncompressed_size: 2,
                })),
            })
            .expect("decode single chunk");
        decoder
            .push(SyncMessage {
                msg: Some(Msg::BulkData(BulkData {
                    start_index: 1,
                    count: 2,
                    data: vec![3, 4, 5, 6, 7],
                    compression: PayloadCompression::Unspecified as i32,
                    uncompressed_size: 5,
                })),
            })
            .expect("decode bulk chunks");
        let received = decoder.finish().expect("all chunks consumed");

        assert_eq!(
            received
                .iter()
                .map(|chunk| chunk.as_ref())
                .collect::<Vec<_>>(),
            vec![&[1, 2][..], &[3, 4][..], &[5, 6, 7][..]]
        );
    }

    #[test]
    fn single_chunk_data_keeps_existing_decompression_behavior() {
        let expected = vec![0x2a; 128 * 1024];
        let encoded = maybe_compress_sync_payload_owned(expected.clone(), None, false)
            .expect("encode compressible chunk");
        assert_eq!(
            encoded.compression,
            PayloadCompression::Zstd as i32,
            "fixture should exercise the decompression path"
        );
        let chunk_sizes = [expected.len()];
        let mut decoder = SyncChunkDecoder::new(&chunk_sizes, vec![0]);

        decoder
            .push(SyncMessage {
                msg: Some(Msg::ChunkData(ChunkData {
                    chunk_index: 0,
                    data: encoded.data,
                    compression: encoded.compression,
                    uncompressed_size: encoded.uncompressed_size,
                })),
            })
            .expect("decode compressed single chunk");
        let received = decoder.finish().expect("all chunks consumed");

        assert_eq!(received.len(), 1);
        assert_eq!(received[0].as_ref(), expected);
    }

    #[test]
    fn compressed_bulk_data_retains_one_shared_backing() {
        let expected = vec![0x33; 128 * 1024];
        let encoded = maybe_compress_sync_payload_owned(expected.clone(), None, false)
            .expect("encode compressible bulk payload");
        assert_eq!(encoded.compression, PayloadCompression::Zstd as i32);
        let mut decoder = SyncChunkDecoder::new(&[64 * 1024, 64 * 1024], vec![0, 1]);

        decoder
            .push(SyncMessage {
                msg: Some(Msg::BulkData(BulkData {
                    start_index: 0,
                    count: 2,
                    data: encoded.data,
                    compression: encoded.compression,
                    uncompressed_size: encoded.uncompressed_size,
                })),
            })
            .expect("decode compressed bulk frame");
        let received = decoder.finish().expect("all chunks consumed");

        let combined = received
            .iter()
            .flat_map(|chunk| chunk.as_ref().iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(combined, expected);
        assert_eq!(received[0].backing_id(), received[1].backing_id());
        assert!(received[0].backing_allocation_bytes() >= expected.len());
    }

    #[test]
    fn rejects_duplicate_out_of_order_gap_zero_count_and_incomplete_eof() {
        fn chunk(index: u32, data: &[u8]) -> SyncMessage {
            SyncMessage {
                msg: Some(Msg::ChunkData(ChunkData {
                    chunk_index: index,
                    data: data.to_vec(),
                    compression: PayloadCompression::Unspecified as i32,
                    uncompressed_size: data.len() as u32,
                })),
            }
        }

        let mut duplicate = SyncChunkDecoder::new(&[1, 1], vec![0, 1]);
        duplicate.push(chunk(0, &[1])).unwrap();
        assert!(duplicate.push(chunk(0, &[1])).is_err());

        let mut out_of_order = SyncChunkDecoder::new(&[1, 1], vec![0, 1]);
        assert!(out_of_order.push(chunk(1, &[2])).is_err());

        let mut gap = SyncChunkDecoder::new(&[1, 1, 1], vec![0, 2]);
        assert!(gap
            .push(SyncMessage {
                msg: Some(Msg::BulkData(BulkData {
                    start_index: 0,
                    count: 2,
                    data: vec![1, 3],
                    compression: PayloadCompression::Unspecified as i32,
                    uncompressed_size: 2,
                })),
            })
            .is_err());

        let mut overlap = SyncChunkDecoder::new(&[1, 1, 1], vec![0, 1, 2]);
        overlap
            .push(SyncMessage {
                msg: Some(Msg::BulkData(BulkData {
                    start_index: 0,
                    count: 2,
                    data: vec![1, 2],
                    compression: PayloadCompression::Unspecified as i32,
                    uncompressed_size: 2,
                })),
            })
            .unwrap();
        assert!(overlap
            .push(SyncMessage {
                msg: Some(Msg::BulkData(BulkData {
                    start_index: 1,
                    count: 2,
                    data: vec![2, 3],
                    compression: PayloadCompression::Unspecified as i32,
                    uncompressed_size: 2,
                })),
            })
            .is_err());
        assert_eq!(overlap.received.len(), 2, "rejected overlap is atomic");

        let mut zero_count = SyncChunkDecoder::new(&[], Vec::new());
        assert!(zero_count
            .push(SyncMessage {
                msg: Some(Msg::BulkData(BulkData {
                    start_index: 0,
                    count: 0,
                    data: Vec::new(),
                    compression: PayloadCompression::Unspecified as i32,
                    uncompressed_size: 0,
                })),
            })
            .is_err());

        let incomplete = SyncChunkDecoder::new(&[1], vec![0]);
        assert!(incomplete.finish().is_err());
    }
}
