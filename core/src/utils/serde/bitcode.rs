use bitcode;
use serde::{Deserialize, Serialize};

pub type Bitcode = Vec<u8>;

pub trait BitcodeSerializable<'a, T: Serialize + Deserialize<'a>> {
    fn to_bitcode(self) -> Result<Vec<u8>, bitcode::Error>
    where
        Self: Sized + Serialize,
    {
        bitcode::serialize(&self)
    }

    fn from_bitcode(buf: &'a [u8]) -> Result<T, bitcode::Error> {
        bitcode::deserialize(buf)
    }
}
