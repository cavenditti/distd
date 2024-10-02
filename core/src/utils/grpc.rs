use tonic::metadata::{Binary, BinaryMetadataValue, MetadataValue};
use uuid::Uuid;

use crate::error::InvalidParameter;

pub fn uuid_to_metadata(uuid: &Uuid) -> MetadataValue<Binary> {
    BinaryMetadataValue::from_bytes(uuid.to_bytes_le().as_ref())
}

pub fn metadata_to_uuid(uuid: &MetadataValue<Binary>) -> Result<Uuid, InvalidParameter> {
    uuid.to_bytes()
        .map_err(InvalidParameter::MetadataBytes)
        .map(|x| Uuid::from_bytes_le(*x.array_chunks::<16>().collect::<Vec<&[u8; 16]>>()[0]))
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::{metadata_to_uuid, uuid_to_metadata};

    #[test]
    fn uuid_metadata_roundtrip() {
        let uuid = Uuid::nil();
        let res = metadata_to_uuid(&uuid_to_metadata(&uuid)).unwrap();

        assert_eq!(uuid, res);

        let uuid = Uuid::new_v4();
        let res = metadata_to_uuid(&uuid_to_metadata(&uuid)).unwrap();

        assert_eq!(uuid, res);
    }
}
