use bytes::Bytes;
use uuid::Uuid;

/// Convert little endian bytes to Uuid
pub fn bytes_to_uuid(x: Bytes) -> Uuid {
    slice_to_uuid(&x)
}

/// Convert little endian slice of bytes to Uuid
pub fn slice_to_uuid(x: &[u8]) -> Uuid {
    Uuid::from_bytes(*x.array_chunks::<16>().collect::<Vec<&[u8; 16]>>()[0])
}
