use bitcode;
use serde::{Deserialize as De, Serialize as Ser};

pub type Bitcode = Vec<u8>;

pub trait Serializable<'a, T: Ser + De<'a>> {
    fn to_bitcode(self) -> Result<Vec<u8>, bitcode::Error>
    where
        Self: Sized + Ser,
    {
        bitcode::serialize(&self)
    }

    fn from_bitcode(buf: &'a [u8]) -> Result<T, bitcode::Error> {
        bitcode::deserialize(buf)
    }
}
