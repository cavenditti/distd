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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use uuid::Uuid;

    use super::bytes_to_uuid;
    use super::slice_to_uuid;

    #[test]
    fn test_bytes_to_uuid() {
        let uuid = Uuid::new_v4();
        let slice = uuid.as_bytes().as_slice();
        let bytes = Bytes::copy_from_slice(slice);
        assert_eq!(bytes_to_uuid(bytes), uuid);
        assert_eq!(slice_to_uuid(slice), uuid);
    }
}
