use std::convert::Infallible;

use crate::chunks::CHUNK_SIZE;

#[must_use]
#[inline(always)]
pub fn merge_hashes(left: &hash::Hash, right: &hash::Hash) -> hash::Hash {
    blake3::hash(
        &left
            .as_bytes()
            .iter()
            .chain(right.as_bytes().iter())
            .cloned()
            .collect::<Vec<u8>>(),
    )
    .into()
}

/// Trait to abstract away the structure used to compute hash-tree
pub trait HashTreeCapable<T, E>
where
    E: std::error::Error,
{
    fn func(&mut self, data: &[u8]) -> Result<T, E>;
    fn merge(&mut self, l: &T, r: &T) -> Result<T, E>;

    fn compute_tree(&mut self, data: &[u8]) -> Result<T, E>
    where
        Self: Sized,
    {
        if data.len() <= CHUNK_SIZE {
            return Ok(self.func(data)?.into());
        }

        // pre-allocate partials vec
        let mut partials: Vec<T> = Vec::with_capacity(data.len() / CHUNK_SIZE + 1);

        // Compute single chunks results
        for chunk in data.chunks(CHUNK_SIZE as usize) {
            partials.push(self.func(chunk)?.into());
        }

        while partials.len() > 1 {
            // to is the destination position, i the first result position
            for (to, i) in (0..partials.len() - 1).step_by(2).enumerate() {
                partials[to] = self.merge(&partials[i], &partials[i + 1])?
            }

            // if there's an element remaining put it in last position
            if partials.len() % 2 != 0 {
                partials.swap_remove(partials.len() / 2 + 1);
                partials.truncate(partials.len() / 2 + 1);
            } else {
                partials.truncate(partials.len() / 2);
            }
        }
        Ok(partials.swap_remove(0).into())
    }
}

/// Wrapper to allow dynamic dispatch
struct DynHashTreeCapable<Func, Merge, T, E>
where
    Func: FnMut(&[u8]) -> Result<T, E>,
    Merge: FnMut(&T, &T) -> Result<T, E>,
    E: std::error::Error,
{
    pub func: Func,
    pub merge: Merge,
}

impl<Func, Merge, T, E> HashTreeCapable<T, E> for DynHashTreeCapable<Func, Merge, T, E>
where
    Func: FnMut(&[u8]) -> Result<T, E>,
    Merge: FnMut(&T, &T) -> Result<T, E>,
    E: std::error::Error,
{
    fn func(&mut self, data: &[u8]) -> Result<T, E> {
        (self.func)(data)
    }

    fn merge(&mut self, l: &T, r: &T) -> Result<T, E> {
        (self.merge)(l, r)
    }
}

/// Compute hash-tree of data
///
/// Used to abstract away the structure used to compute hash-tree, because storage may be doing almost the same
/// thing but inserting nodes in the process
pub fn compute_tree<Func, Merge, T, E>(func: Func, merge: Merge, data: &[u8]) -> Result<T, E>
where
    Func: FnMut(&[u8]) -> Result<T, E>,
    Merge: FnMut(&T, &T) -> Result<T, E>,
    E: std::error::Error,
{
    DynHashTreeCapable { func, merge }.compute_tree(data)
}

/// Hashing function. Uses BLAKE3 but without Subtree-freeness
#[must_use]
pub fn hash(data: &[u8]) -> hash::Hash {
    compute_tree(
        |x| -> Result<hash::Hash, Infallible> { Ok(blake3::hash(x).into()) },
        |l, r| Ok(merge_hashes(l, r)),
        data,
    )
    .unwrap()
}

pub use hash::Hash;
pub use hash::HexError;

/// Code taken from blake3 crate with minor changes
pub mod hash {
    use std::fmt;

    use blake3::OUT_LEN;
    use serde::{Deserialize, Serialize};

    /// Reimplementation of blake3::Hash without constant-time comparisons
    ///
    /// Constant-time equality check is not needed for this use-case and only hurts performance
    /// Code is almost identical to blake3::Hash
    #[derive(Clone, Copy, Serialize, Deserialize, Hash, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Hash([u8; OUT_LEN]);

    impl Hash {
        /// The raw bytes of the `Hash`. Note that byte arrays don't provide
        /// constant-time equality checking, so if  you need to compare hashes,
        /// prefer the `Hash` type.
        #[inline]
        pub const fn as_bytes(&self) -> &[u8; OUT_LEN] {
            &self.0
        }

        /// Create a `Hash` from its raw bytes representation.
        pub const fn from_bytes(bytes: [u8; OUT_LEN]) -> Self {
            Self(bytes)
        }

        /// Convert to `blake3::Hash`
        pub fn to_blake3_hash(self) -> blake3::Hash {
            blake3::Hash::from_bytes(self.into())
        }

        /// Decode a `Hash` from hexadecimal. Both uppercase and lowercase ASCII
        /// bytes are supported.
        ///
        /// Any byte outside the ranges `'0'...'9'`, `'a'...'f'`, and `'A'...'F'`
        /// results in an error. An input length other than 64 also results in an
        /// error.
        ///
        /// Note that `Hash` also implements `FromStr`, so `Hash::from_hex("...")`
        /// is equivalent to `"...".parse()`.
        pub fn from_hex(hex: impl AsRef<[u8]>) -> Result<Self, HexError> {
            fn hex_val(byte: u8) -> Result<u8, HexError> {
                match byte {
                    b'A'..=b'F' => Ok(byte - b'A' + 10),
                    b'a'..=b'f' => Ok(byte - b'a' + 10),
                    b'0'..=b'9' => Ok(byte - b'0'),
                    _ => Err(HexError(HexErrorInner::InvalidByte(byte))),
                }
            }
            let hex_bytes: &[u8] = hex.as_ref();
            if hex_bytes.len() != OUT_LEN * 2 {
                return Err(HexError(HexErrorInner::InvalidLen(hex_bytes.len())));
            }
            let mut hash_bytes: [u8; OUT_LEN] = [0; OUT_LEN];
            for i in 0..OUT_LEN {
                hash_bytes[i] = 16 * hex_val(hex_bytes[2 * i])? + hex_val(hex_bytes[2 * i + 1])?;
            }
            Ok(Hash::from(hash_bytes))
        }
    }

    impl From<blake3::Hash> for Hash {
        #[inline]
        fn from(hash: blake3::Hash) -> Self {
            Self::from_bytes(hash.into())
        }
    }

    impl From<[u8; OUT_LEN]> for Hash {
        #[inline]
        fn from(bytes: [u8; OUT_LEN]) -> Self {
            Self::from_bytes(bytes)
        }
    }

    impl From<Hash> for [u8; OUT_LEN] {
        #[inline]
        fn from(hash: Hash) -> Self {
            hash.0
        }
    }

    impl core::str::FromStr for Hash {
        type Err = HexError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            Hash::from_hex(s)
        }
    }

    impl fmt::Display for Hash {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            // Formatting field as `&str` to reduce code size since the `Debug`
            // dynamic dispatch table for `&str` is likely needed elsewhere already,
            // but that for `ArrayString<[u8; 64]>` is not.
            let hex = self.to_blake3_hash().to_hex();
            let hex: &str = hex.as_str();

            f.write_str(hex)
        }
    }

    impl fmt::Debug for Hash {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            // Formatting field as `&str` to reduce code size since the `Debug`
            // dynamic dispatch table for `&str` is likely needed elsewhere already,
            // but that for `ArrayString<[u8; 64]>` is not.
            let hex = self.to_blake3_hash().to_hex();
            let hex: &str = hex.as_str();

            f.debug_tuple("Hash").field(&hex).finish()
        }
    }

    /// Reimplementation of the error type for [`Hash::from_hex`].
    ///
    /// The `.to_string()` representation of this error currently distinguishes between bad length
    /// errors and bad character errors. This is to help with logging and debugging, but it isn't a
    /// stable API detail, and it may change at any time.
    #[derive(Clone, Debug)]
    pub struct HexError(HexErrorInner);

    #[derive(Clone, Debug)]
    enum HexErrorInner {
        InvalidByte(u8),
        InvalidLen(usize),
    }

    impl fmt::Display for HexError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self.0 {
                HexErrorInner::InvalidByte(byte) => {
                    if byte < 128 {
                        write!(f, "invalid hex character: {:?}", byte as char)
                    } else {
                        write!(f, "invalid hex character: 0x{:x}", byte)
                    }
                }
                HexErrorInner::InvalidLen(len) => {
                    write!(f, "expected 64 hex bytes, received {}", len)
                }
            }
        }
    }

    //#[cfg(feature = "std")]
    impl std::error::Error for HexError {}
}

#[cfg(test)]
mod tests {
    use crate::chunks::CHUNK_SIZE;
    use crate::hash::merge_hashes;

    use super::hash;
    use super::Hash;

    #[test]
    fn test_blake3_one_chunk() {
        let data = b"some random data";
        assert_eq!(Hash::from(blake3::hash(data)), hash(data));
    }

    #[test]
    /// We're doing it differently, so they should differ (unless blake3 CHUNK_SIZE == our CHUNK_SIZE + 1)
    fn test_blake3_multiple_chunks() {
        let data = [1u8; CHUNK_SIZE + 1];
        assert_ne!(Hash::from(blake3::hash(&data)), hash(&data));

        let h = merge_hashes(
            &blake3::hash(&data[..CHUNK_SIZE]).into(),
            &blake3::hash(&data[CHUNK_SIZE..]).into(),
        );

        assert_eq!(hash(&data), h);
    }
}
