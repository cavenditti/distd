use std::io::Cursor;

use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};

pub type Msgpack = Vec<u8>;

pub trait MsgPackSerializable<'a, T: Serialize + Deserialize<'a>> {
    fn to_msgpack(self) -> Result<Vec<u8>, rmp_serde::encode::Error>
    where
        Self: Sized + Serialize,
    {
        let mut buf = Vec::new();
        let mut serializer =
            Serializer::new(&mut buf).with_bytes(rmp_serde::config::BytesMode::ForceAll);
        self.serialize(&mut serializer)?;
        Ok(buf)
    }

    fn from_msgpack(buf: Vec<u8>) -> Result<T, rmp_serde::decode::Error> {
        let cur = Cursor::new(&buf[..]);
        let mut de = Deserializer::new(cur);
        Deserialize::deserialize(&mut de)
    }
}
