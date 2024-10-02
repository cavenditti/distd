use crate::chunks::CHUNK_SIZE;

#[must_use]
pub fn merge_hashes(left: &hash::Hash, right: &hash::Hash) -> hash::Hash {
    let mut combined_hashes = left.as_bytes().to_vec();
    combined_hashes.extend(right.as_bytes());
    blake3::hash(&combined_hashes).into()
}

/// Hashing function. Uses BLAKE3 but without Subtree-freeness
#[must_use]
pub fn hash(data: &[u8]) -> hash::Hash {
    fn partial_tree(slices: &[&[u8]]) -> hash::Hash {
        /*
        println!(
            "[HASH-NE] {}, {:?}",
            slices.len(),
            slices.iter().map(|x| x.len()).collect::<Vec<usize>>()
        );
        */
        match slices.len() {
            //0 => panic!("Requested hash of empty slice: Should never happen"),
            0 => blake3::hash(b"").into(), // Why it's needed?
            1 => blake3::hash(slices[0]).into(),
            _ => merge_hashes(
                &partial_tree(&slices[..slices.len() / 2]),
                &partial_tree(&slices[slices.len() / 2..]),
            ),
        }
        //println!("[HASH-NE] HASH: {}", x);
    }

    let chunks: Vec<&[u8]> = data.chunks(CHUNK_SIZE as usize).collect();
    partial_tree(&chunks)
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
    use super::hash;
    use super::Hash;

    #[test]
    fn test_blake3_one_chunk() {
        let data = b"some random data";
        assert_eq!(Hash::from(blake3::hash(data)), hash(data));
    }

    #[test]
    /// We're doing it differently, so they should differ
    fn test_blake3_multiple_chunks() {
        let data = [0u8; 10000];
        assert_ne!(Hash::from(blake3::hash(&data)), hash(&data));
    }
}
