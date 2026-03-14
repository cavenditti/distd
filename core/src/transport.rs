use std::path::Path;

use thiserror::Error;

use crate::chunks::path_likely_precompressed;

pub const SYNC_PAYLOAD_COMPRESSION_NONE: i32 = 0;
pub const SYNC_PAYLOAD_COMPRESSION_ZSTD: i32 = 1;
const SYNC_PAYLOAD_COMPRESSION_MIN_BYTES: usize = 64 * 1024;
const SYNC_PAYLOAD_COMPRESSION_MIN_SAVINGS_BYTES: usize = 8 * 1024;
const SYNC_PAYLOAD_ZSTD_LEVEL: i32 = 1;

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

pub fn maybe_compress_sync_payload(
    data: &[u8],
    primary_path: Option<&Path>,
    likely_precompressed: bool,
) -> Result<EncodedSyncPayload, PayloadCompressionError> {
    let uncompressed_size = u32::try_from(data.len()).map_err(|_| PayloadCompressionError::SizeTooLarge)?;

    if std::env::var_os("DISTD_DISABLE_SYNC_PAYLOAD_COMPRESSION").is_some() {
        return Ok(EncodedSyncPayload {
            compression: SYNC_PAYLOAD_COMPRESSION_NONE,
            data: data.to_vec(),
            uncompressed_size,
        });
    }

    if data.len() < SYNC_PAYLOAD_COMPRESSION_MIN_BYTES {
        return Ok(EncodedSyncPayload {
            compression: SYNC_PAYLOAD_COMPRESSION_NONE,
            data: data.to_vec(),
            uncompressed_size,
        });
    }

    if likely_precompressed || primary_path.is_some_and(path_likely_precompressed) {
        return Ok(EncodedSyncPayload {
            compression: SYNC_PAYLOAD_COMPRESSION_NONE,
            data: data.to_vec(),
            uncompressed_size,
        });
    }

    let compressed = zstd::bulk::compress(data, SYNC_PAYLOAD_ZSTD_LEVEL)
        .map_err(|err| PayloadCompressionError::Compress(err.to_string()))?;

    if compressed.len() + SYNC_PAYLOAD_COMPRESSION_MIN_SAVINGS_BYTES > data.len() {
        return Ok(EncodedSyncPayload {
            compression: SYNC_PAYLOAD_COMPRESSION_NONE,
            data: data.to_vec(),
            uncompressed_size,
        });
    }

    Ok(EncodedSyncPayload {
        compression: SYNC_PAYLOAD_COMPRESSION_ZSTD,
        data: compressed,
        uncompressed_size,
    })
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

    use super::TransportOp;
    use super::{decode_sync_payload, maybe_compress_sync_payload, SYNC_PAYLOAD_COMPRESSION_NONE, SYNC_PAYLOAD_COMPRESSION_ZSTD};

    #[test]
    fn op_roundtrip() {
        assert_eq!(TransportOp::try_from(u8::from(TransportOp::Register)).unwrap(), TransportOp::Register);
        assert_eq!(TransportOp::try_from(u8::from(TransportOp::Fetch)).unwrap(), TransportOp::Fetch);
        assert_eq!(TransportOp::try_from(u8::from(TransportOp::Sync)).unwrap(), TransportOp::Sync);
    }

    #[test]
    fn skips_small_payloads() {
        let payload = vec![b'a'; 4096];
        let encoded = maybe_compress_sync_payload(&payload, Some(Path::new("artifact.tar")), false).unwrap();

        assert_eq!(encoded.compression, SYNC_PAYLOAD_COMPRESSION_NONE);
        assert_eq!(encoded.data, payload);
    }

    #[test]
    fn skips_known_precompressed_paths() {
        let payload = vec![b'a'; 256 * 1024];
        let encoded = maybe_compress_sync_payload(&payload, Some(Path::new("layer.tar.gz")), false).unwrap();

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
        let encoded = maybe_compress_sync_payload(&payload, Some(Path::new("rootfs.tar")), false).unwrap();

        assert_eq!(encoded.compression, SYNC_PAYLOAD_COMPRESSION_ZSTD);
        assert!(encoded.data.len() < payload.len());
        assert_eq!(decode_sync_payload(encoded.compression, &encoded.data, encoded.uncompressed_size).unwrap(), payload);
    }
}