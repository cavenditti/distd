use std::path::Path;

use thiserror::Error;

use crate::chunks::path_likely_precompressed;

pub const WIRE_PREFACE_BYTES: usize = 12;
pub const WIRE_REJECTION_BYTES: usize = 12;
pub const WIRE_PREFACE_FORMAT_V1: u16 = 1;
pub const WIRE_OPERATION_VERSION_V1: u16 = 1;
pub const WIRE_PROTOBUF_VERSION_V1: u16 = 1;
const WIRE_PREFACE_DOMAIN: [u8; 4] = *b"DSWP";
pub const WIRE_REJECTION_DOMAIN: [u8; 4] = *b"DSWR";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireVersionDeclaration {
    pub preface_format: u16,
    pub operation_version: u16,
    pub protobuf_version: u16,
}

impl WireVersionDeclaration {
    pub const CURRENT: Self = Self {
        preface_format: WIRE_PREFACE_FORMAT_V1,
        operation_version: WIRE_OPERATION_VERSION_V1,
        protobuf_version: WIRE_PROTOBUF_VERSION_V1,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum WireRejectionStatus {
    UnsupportedPrefaceVersion = 1,
    UnsupportedOperationVersion = 2,
    UnsupportedProtobufVersion = 3,
    UnsupportedVersionPair = 4,
    MalformedPreface = 5,
}

impl TryFrom<u16> for WireRejectionStatus {
    type Error = WireCompatibilityError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::UnsupportedPrefaceVersion),
            2 => Ok(Self::UnsupportedOperationVersion),
            3 => Ok(Self::UnsupportedProtobufVersion),
            4 => Ok(Self::UnsupportedVersionPair),
            5 => Ok(Self::MalformedPreface),
            _ => Err(WireCompatibilityError::MalformedRejection),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireRejection {
    pub status: WireRejectionStatus,
    pub supported_operation_version: u16,
    pub supported_protobuf_version: u16,
}

impl WireRejection {
    pub const fn current(status: WireRejectionStatus) -> Self {
        Self {
            status,
            supported_operation_version: WIRE_OPERATION_VERSION_V1,
            supported_protobuf_version: WIRE_PROTOBUF_VERSION_V1,
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum WireCompatibilityError {
    #[error("malformed wire compatibility preface")]
    MalformedPreface,

    #[error("unsupported wire preface version")]
    UnsupportedPrefaceVersion,

    #[error("unsupported wire operation version")]
    UnsupportedOperationVersion,

    #[error("unsupported protobuf contract version")]
    UnsupportedProtobufVersion,

    #[error("unsupported wire operation/protobuf version pair")]
    UnsupportedVersionPair,

    #[error("malformed wire compatibility rejection")]
    MalformedRejection,

    #[error("peer rejected the wire contract: {0:?}")]
    Rejected(WireRejectionStatus),
}

impl WireCompatibilityError {
    pub const fn rejection_status(self) -> WireRejectionStatus {
        match self {
            Self::UnsupportedPrefaceVersion => WireRejectionStatus::UnsupportedPrefaceVersion,
            Self::UnsupportedOperationVersion => WireRejectionStatus::UnsupportedOperationVersion,
            Self::UnsupportedProtobufVersion => WireRejectionStatus::UnsupportedProtobufVersion,
            Self::UnsupportedVersionPair => WireRejectionStatus::UnsupportedVersionPair,
            Self::Rejected(status) => status,
            Self::MalformedPreface | Self::MalformedRejection => {
                WireRejectionStatus::MalformedPreface
            }
        }
    }
}

pub fn encode_wire_preface(declaration: WireVersionDeclaration) -> [u8; WIRE_PREFACE_BYTES] {
    let mut encoded = [0u8; WIRE_PREFACE_BYTES];
    encoded[..4].copy_from_slice(&WIRE_PREFACE_DOMAIN);
    encoded[4..6].copy_from_slice(&declaration.preface_format.to_be_bytes());
    encoded[6..8].copy_from_slice(&declaration.operation_version.to_be_bytes());
    encoded[8..10].copy_from_slice(&declaration.protobuf_version.to_be_bytes());
    encoded
}

pub fn decode_wire_preface(
    encoded: &[u8; WIRE_PREFACE_BYTES],
) -> Result<WireVersionDeclaration, WireCompatibilityError> {
    if encoded[..4] != WIRE_PREFACE_DOMAIN || encoded[10..12] != [0, 0] {
        return Err(WireCompatibilityError::MalformedPreface);
    }

    let declaration = WireVersionDeclaration {
        preface_format: u16::from_be_bytes([encoded[4], encoded[5]]),
        operation_version: u16::from_be_bytes([encoded[6], encoded[7]]),
        protobuf_version: u16::from_be_bytes([encoded[8], encoded[9]]),
    };
    if declaration.preface_format != WIRE_PREFACE_FORMAT_V1 {
        return Err(WireCompatibilityError::UnsupportedPrefaceVersion);
    }

    let operation_supported = declaration.operation_version == WIRE_OPERATION_VERSION_V1;
    let protobuf_supported = declaration.protobuf_version == WIRE_PROTOBUF_VERSION_V1;
    match (operation_supported, protobuf_supported) {
        (true, true) => Ok(declaration),
        (false, true) => Err(WireCompatibilityError::UnsupportedOperationVersion),
        (true, false) => Err(WireCompatibilityError::UnsupportedProtobufVersion),
        (false, false) => Err(WireCompatibilityError::UnsupportedVersionPair),
    }
}

pub fn encode_wire_rejection(rejection: WireRejection) -> [u8; WIRE_REJECTION_BYTES] {
    let mut encoded = [0u8; WIRE_REJECTION_BYTES];
    encoded[..4].copy_from_slice(&WIRE_REJECTION_DOMAIN);
    encoded[4..6].copy_from_slice(&WIRE_PREFACE_FORMAT_V1.to_be_bytes());
    encoded[6..8].copy_from_slice(&(rejection.status as u16).to_be_bytes());
    encoded[8..10].copy_from_slice(&rejection.supported_operation_version.to_be_bytes());
    encoded[10..12].copy_from_slice(&rejection.supported_protobuf_version.to_be_bytes());
    encoded
}

pub fn decode_wire_rejection(
    encoded: &[u8; WIRE_REJECTION_BYTES],
) -> Result<WireRejection, WireCompatibilityError> {
    if encoded[..4] != WIRE_REJECTION_DOMAIN
        || u16::from_be_bytes([encoded[4], encoded[5]]) != WIRE_PREFACE_FORMAT_V1
    {
        return Err(WireCompatibilityError::MalformedRejection);
    }

    Ok(WireRejection {
        status: WireRejectionStatus::try_from(u16::from_be_bytes([encoded[6], encoded[7]]))?,
        supported_operation_version: u16::from_be_bytes([encoded[8], encoded[9]]),
        supported_protobuf_version: u16::from_be_bytes([encoded[10], encoded[11]]),
    })
}

pub const SYNC_PAYLOAD_COMPRESSION_NONE: i32 = 0;
pub const SYNC_PAYLOAD_COMPRESSION_ZSTD: i32 = 1;
const SYNC_PAYLOAD_COMPRESSION_MIN_BYTES: usize = 64 * 1024;
const SYNC_PAYLOAD_COMPRESSION_MIN_SAVINGS_BYTES: usize = 8 * 1024;
const SYNC_PAYLOAD_ZSTD_LEVEL: i32 = 1;
const SYNC_PAYLOAD_COMPRESSION_PROBE_WINDOW_BYTES: usize = 32 * 1024;
pub const MAX_SYNC_DECOMPRESSED_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransportOp {
    Register = 1,
    Fetch = 2,
    Sync = 3,
}

#[derive(Debug, Error)]
pub enum TransportOpError {
    #[error("unknown transport op {0}")]
    Unknown(u8),
}

#[derive(Debug, Error)]
pub enum PayloadCompressionError {
    #[error("payload is too large to encode its uncompressed size")]
    SizeTooLarge,

    #[error("unknown payload compression codec {0}")]
    UnknownCodec(i32),

    #[error("payload compression failed: {0}")]
    Compress(String),

    #[error("payload decompression failed: {0}")]
    Decompress(String),

    #[error("payload size mismatch: expected {expected} bytes, got {actual}")]
    SizeMismatch { expected: usize, actual: usize },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedSyncPayload {
    pub compression: i32,
    pub data: Vec<u8>,
    pub uncompressed_size: u32,
}

fn uncompressed_payload(data: &[u8], uncompressed_size: u32) -> EncodedSyncPayload {
    EncodedSyncPayload {
        compression: SYNC_PAYLOAD_COMPRESSION_NONE,
        data: data.to_vec(),
        uncompressed_size,
    }
}

fn uncompressed_payload_owned(data: Vec<u8>, uncompressed_size: u32) -> EncodedSyncPayload {
    EncodedSyncPayload {
        compression: SYNC_PAYLOAD_COMPRESSION_NONE,
        data,
        uncompressed_size,
    }
}

fn compression_probe_sample(data: &[u8]) -> Vec<u8> {
    if data.len() <= SYNC_PAYLOAD_COMPRESSION_PROBE_WINDOW_BYTES * 3 {
        return data.to_vec();
    }

    let window = SYNC_PAYLOAD_COMPRESSION_PROBE_WINDOW_BYTES;
    let middle_start = data.len() / 2 - window / 2;
    let end_start = data.len() - window;
    let mut sample = Vec::with_capacity(window * 3);
    sample.extend_from_slice(&data[..window]);
    sample.extend_from_slice(&data[middle_start..middle_start + window]);
    sample.extend_from_slice(&data[end_start..]);
    sample
}

fn probe_payload_compressibility(data: &[u8]) -> Result<bool, PayloadCompressionError> {
    let sample = compression_probe_sample(data);
    let compressed = zstd::bulk::compress(&sample, SYNC_PAYLOAD_ZSTD_LEVEL)
        .map_err(|err| PayloadCompressionError::Compress(err.to_string()))?;

    Ok(compressed.len() + SYNC_PAYLOAD_COMPRESSION_MIN_SAVINGS_BYTES <= sample.len())
}

pub fn maybe_compress_sync_payload(
    data: &[u8],
    primary_path: Option<&Path>,
    likely_precompressed: bool,
) -> Result<EncodedSyncPayload, PayloadCompressionError> {
    let uncompressed_size =
        u32::try_from(data.len()).map_err(|_| PayloadCompressionError::SizeTooLarge)?;

    if std::env::var_os("DISTD_DISABLE_SYNC_PAYLOAD_COMPRESSION").is_some() {
        return Ok(uncompressed_payload(data, uncompressed_size));
    }

    if data.len() < SYNC_PAYLOAD_COMPRESSION_MIN_BYTES {
        return Ok(uncompressed_payload(data, uncompressed_size));
    }

    if likely_precompressed || primary_path.is_some_and(path_likely_precompressed) {
        return Ok(uncompressed_payload(data, uncompressed_size));
    }

    if !probe_payload_compressibility(data)? {
        return Ok(uncompressed_payload(data, uncompressed_size));
    }

    let compressed = zstd::bulk::compress(data, SYNC_PAYLOAD_ZSTD_LEVEL)
        .map_err(|err| PayloadCompressionError::Compress(err.to_string()))?;

    if compressed.len() + SYNC_PAYLOAD_COMPRESSION_MIN_SAVINGS_BYTES > data.len() {
        return Ok(uncompressed_payload(data, uncompressed_size));
    }

    Ok(EncodedSyncPayload {
        compression: SYNC_PAYLOAD_COMPRESSION_ZSTD,
        data: compressed,
        uncompressed_size,
    })
}

/// Like [`maybe_compress_sync_payload`] but takes ownership of the buffer to
/// avoid a copy when the data is sent uncompressed.
pub fn maybe_compress_sync_payload_owned(
    data: Vec<u8>,
    primary_path: Option<&Path>,
    likely_precompressed: bool,
) -> Result<EncodedSyncPayload, PayloadCompressionError> {
    let uncompressed_size =
        u32::try_from(data.len()).map_err(|_| PayloadCompressionError::SizeTooLarge)?;

    if std::env::var_os("DISTD_DISABLE_SYNC_PAYLOAD_COMPRESSION").is_some() {
        return Ok(uncompressed_payload_owned(data, uncompressed_size));
    }

    if data.len() < SYNC_PAYLOAD_COMPRESSION_MIN_BYTES {
        return Ok(uncompressed_payload_owned(data, uncompressed_size));
    }

    if likely_precompressed || primary_path.is_some_and(path_likely_precompressed) {
        return Ok(uncompressed_payload_owned(data, uncompressed_size));
    }

    if !probe_payload_compressibility(&data)? {
        return Ok(uncompressed_payload_owned(data, uncompressed_size));
    }

    let compressed = zstd::bulk::compress(&data, SYNC_PAYLOAD_ZSTD_LEVEL)
        .map_err(|err| PayloadCompressionError::Compress(err.to_string()))?;

    if compressed.len() + SYNC_PAYLOAD_COMPRESSION_MIN_SAVINGS_BYTES > data.len() {
        return Ok(uncompressed_payload_owned(data, uncompressed_size));
    }

    Ok(EncodedSyncPayload {
        compression: SYNC_PAYLOAD_COMPRESSION_ZSTD,
        data: compressed,
        uncompressed_size,
    })
}

/// Like [`decode_sync_payload`] but takes ownership of the data to avoid a
/// copy when the payload is uncompressed.
pub fn decode_sync_payload_owned(
    compression: i32,
    data: Vec<u8>,
    uncompressed_size: u32,
) -> Result<Vec<u8>, PayloadCompressionError> {
    let expected_size = if uncompressed_size == 0 {
        data.len()
    } else {
        uncompressed_size as usize
    };
    if expected_size > MAX_SYNC_DECOMPRESSED_BYTES {
        return Err(PayloadCompressionError::SizeTooLarge);
    }

    match compression {
        SYNC_PAYLOAD_COMPRESSION_NONE => {
            if data.len() != expected_size {
                return Err(PayloadCompressionError::SizeMismatch {
                    expected: expected_size,
                    actual: data.len(),
                });
            }
            Ok(data)
        }
        SYNC_PAYLOAD_COMPRESSION_ZSTD => {
            let decoded = zstd::bulk::decompress(&data, expected_size)
                .map_err(|err| PayloadCompressionError::Decompress(err.to_string()))?;
            if decoded.len() != expected_size {
                return Err(PayloadCompressionError::SizeMismatch {
                    expected: expected_size,
                    actual: decoded.len(),
                });
            }
            Ok(decoded)
        }
        other => Err(PayloadCompressionError::UnknownCodec(other)),
    }
}

pub fn decode_sync_payload(
    compression: i32,
    data: &[u8],
    uncompressed_size: u32,
) -> Result<Vec<u8>, PayloadCompressionError> {
    let expected_size = if uncompressed_size == 0 {
        data.len()
    } else {
        uncompressed_size as usize
    };
    if expected_size > MAX_SYNC_DECOMPRESSED_BYTES {
        return Err(PayloadCompressionError::SizeTooLarge);
    }

    match compression {
        SYNC_PAYLOAD_COMPRESSION_NONE => {
            if data.len() != expected_size {
                return Err(PayloadCompressionError::SizeMismatch {
                    expected: expected_size,
                    actual: data.len(),
                });
            }
            Ok(data.to_vec())
        }
        SYNC_PAYLOAD_COMPRESSION_ZSTD => {
            let decoded = zstd::bulk::decompress(data, expected_size)
                .map_err(|err| PayloadCompressionError::Decompress(err.to_string()))?;
            if decoded.len() != expected_size {
                return Err(PayloadCompressionError::SizeMismatch {
                    expected: expected_size,
                    actual: decoded.len(),
                });
            }
            Ok(decoded)
        }
        other => Err(PayloadCompressionError::UnknownCodec(other)),
    }
}

impl From<TransportOp> for u8 {
    fn from(value: TransportOp) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for TransportOp {
    type Error = TransportOpError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Register),
            2 => Ok(Self::Fetch),
            3 => Ok(Self::Sync),
            other => Err(TransportOpError::Unknown(other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use prost::Message as _;
    use rand::{rngs::StdRng, RngCore, SeedableRng};

    use crate::proto::{
        sync_message::Msg, ClientKeepAlive, ClientRegister, ManifestRequest, ManifestResponse,
        ServerMetadata, SyncMessage,
    };

    use super::{
        decode_sync_payload, maybe_compress_sync_payload, SYNC_PAYLOAD_COMPRESSION_NONE,
        SYNC_PAYLOAD_COMPRESSION_ZSTD,
    };
    use super::{
        decode_wire_preface, decode_wire_rejection, encode_wire_preface, encode_wire_rejection,
        TransportOp, WireCompatibilityError, WireRejection, WireRejectionStatus,
        WireVersionDeclaration,
    };

    #[test]
    fn op_roundtrip() {
        assert_eq!(
            TransportOp::try_from(u8::from(TransportOp::Register)).unwrap(),
            TransportOp::Register
        );
        assert_eq!(
            TransportOp::try_from(u8::from(TransportOp::Fetch)).unwrap(),
            TransportOp::Fetch
        );
        assert_eq!(
            TransportOp::try_from(u8::from(TransportOp::Sync)).unwrap(),
            TransportOp::Sync
        );
        for reserved in [0, 4, u8::MAX] {
            assert!(TransportOp::try_from(reserved).is_err());
        }
    }

    #[test]
    fn wire_preface_v1_golden_and_compatibility_matrix_are_exact() {
        let golden = [
            0x44, 0x53, 0x57, 0x50, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00,
        ];
        assert_eq!(encode_wire_preface(WireVersionDeclaration::CURRENT), golden);
        assert_eq!(
            decode_wire_preface(&golden).unwrap(),
            WireVersionDeclaration::CURRENT
        );

        for (operation, protobuf, expected) in [
            (0, 1, WireCompatibilityError::UnsupportedOperationVersion),
            (2, 1, WireCompatibilityError::UnsupportedOperationVersion),
            (1, 0, WireCompatibilityError::UnsupportedProtobufVersion),
            (1, 2, WireCompatibilityError::UnsupportedProtobufVersion),
            (0, 0, WireCompatibilityError::UnsupportedVersionPair),
            (2, 2, WireCompatibilityError::UnsupportedVersionPair),
        ] {
            let encoded = encode_wire_preface(WireVersionDeclaration {
                preface_format: 1,
                operation_version: operation,
                protobuf_version: protobuf,
            });
            assert_eq!(decode_wire_preface(&encoded), Err(expected));
        }

        let mut unsupported_preface = golden;
        unsupported_preface[5] = 2;
        assert_eq!(
            decode_wire_preface(&unsupported_preface),
            Err(WireCompatibilityError::UnsupportedPrefaceVersion)
        );
        for offset in [0, 3, 10, 11] {
            let mut malformed = golden;
            malformed[offset] ^= 1;
            assert_eq!(
                decode_wire_preface(&malformed),
                Err(WireCompatibilityError::MalformedPreface)
            );
        }
    }

    #[test]
    fn every_wire_rejection_status_has_a_frozen_v1_golden() {
        for status in [
            WireRejectionStatus::UnsupportedPrefaceVersion,
            WireRejectionStatus::UnsupportedOperationVersion,
            WireRejectionStatus::UnsupportedProtobufVersion,
            WireRejectionStatus::UnsupportedVersionPair,
            WireRejectionStatus::MalformedPreface,
        ] {
            let encoded = encode_wire_rejection(WireRejection::current(status));
            assert_eq!(&encoded[..6], &[0x44, 0x53, 0x57, 0x52, 0x00, 0x01]);
            assert_eq!(&encoded[6..8], &(status as u16).to_be_bytes());
            assert_eq!(&encoded[8..], &[0x00, 0x01, 0x00, 0x01]);
            assert_eq!(
                decode_wire_rejection(&encoded).unwrap(),
                WireRejection::current(status)
            );
        }

        let mut reserved_status = encode_wire_rejection(WireRejection::current(
            WireRejectionStatus::MalformedPreface,
        ));
        reserved_status[6..8].copy_from_slice(&0u16.to_be_bytes());
        assert_eq!(
            decode_wire_rejection(&reserved_status),
            Err(WireCompatibilityError::MalformedRejection)
        );
    }

    #[test]
    fn protobuf_v1_request_and_response_goldens_cover_every_operation() {
        let register = ClientRegister {
            name: "n".to_string(),
            version: "1".to_string(),
            uuid: None,
            enrollment_token: None,
        };
        assert_eq!(
            register.encode_to_vec(),
            [0x0a, 0x01, b'n', 0x12, 0x01, b'1']
        );

        let fetch = ClientKeepAlive {};
        assert!(fetch.encode_to_vec().is_empty());

        let sync_request = ManifestRequest {
            artifact_id: "a".to_string(),
            version: Some(1),
        };
        assert_eq!(sync_request.encode_to_vec(), [0x0a, 0x01, b'a', 0x10, 0x01]);

        let metadata = ServerMetadata {
            serialized: b"m".to_vec(),
            uuid: None,
        };
        assert_eq!(metadata.encode_to_vec(), [0x0a, 0x01, b'm']);

        let sync_response = SyncMessage {
            msg: Some(Msg::ManifestResponse(ManifestResponse {
                artifact_id: "a".to_string(),
                ..ManifestResponse::default()
            })),
        };
        assert_eq!(
            sync_response.encode_to_vec(),
            [0x12, 0x03, 0x0a, 0x01, b'a']
        );
    }

    #[test]
    fn protobuf_v1_schema_source_is_hash_pinned() {
        assert_eq!(
            crate::hash::hash(include_bytes!("../proto/distd.proto")).to_string(),
            "cb8bf604924e013182f2072daaa688c69d3ab0c55f66affbf5122f0055f8106d"
        );
    }

    #[test]
    fn skips_small_payloads() {
        let payload = vec![b'a'; 4096];
        let encoded =
            maybe_compress_sync_payload(&payload, Some(Path::new("artifact.tar")), false).unwrap();

        assert_eq!(encoded.compression, SYNC_PAYLOAD_COMPRESSION_NONE);
        assert_eq!(encoded.data, payload);
    }

    #[test]
    fn skips_known_precompressed_paths() {
        let payload = vec![b'a'; 256 * 1024];
        let encoded =
            maybe_compress_sync_payload(&payload, Some(Path::new("layer.tar.gz")), false).unwrap();

        assert_eq!(encoded.compression, SYNC_PAYLOAD_COMPRESSION_NONE);
        assert_eq!(encoded.data, payload);
    }

    #[test]
    fn skips_precompressed_hint() {
        let payload = vec![b'a'; 256 * 1024];
        let encoded = maybe_compress_sync_payload(&payload, None, true).unwrap();

        assert_eq!(encoded.compression, SYNC_PAYLOAD_COMPRESSION_NONE);
        assert_eq!(encoded.data, payload);
    }

    #[test]
    fn compresses_large_compressible_payloads() {
        let payload = vec![b'a'; 512 * 1024];
        let encoded =
            maybe_compress_sync_payload(&payload, Some(Path::new("rootfs.tar")), false).unwrap();

        assert_eq!(encoded.compression, SYNC_PAYLOAD_COMPRESSION_ZSTD);
        assert!(encoded.data.len() < payload.len());
        assert_eq!(
            decode_sync_payload(
                encoded.compression,
                &encoded.data,
                encoded.uncompressed_size
            )
            .unwrap(),
            payload
        );
    }

    #[test]
    fn skips_large_incompressible_payloads() {
        let mut payload = vec![0u8; 512 * 1024];
        StdRng::seed_from_u64(0xD157_DA7A).fill_bytes(&mut payload);

        let encoded =
            maybe_compress_sync_payload(&payload, Some(Path::new("artifact.bin")), false).unwrap();

        assert_eq!(encoded.compression, SYNC_PAYLOAD_COMPRESSION_NONE);
        assert_eq!(encoded.data, payload);
    }
}
