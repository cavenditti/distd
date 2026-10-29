use redb::{TypeName, Value};

/// A borrowed view of redb 2.2's existing `Vec<u8>` encoding.
///
/// Using redb's built-in `&[u8]` would change both the type identifier and
/// encoding. Keep the original variable-length count prefix so existing
/// databases remain readable in both directions, without migration.
#[derive(Debug)]
pub(super) struct ByteSlice;

impl Value for ByteSlice {
    type SelfType<'a> = &'a [u8];
    type AsBytes<'a> = Vec<u8>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> &'a [u8]
    where
        Self: 'a,
    {
        let (length, offset) = match data[0] {
            0..=253 => (usize::from(data[0]), 1),
            254 => (
                usize::from(u16::from_le_bytes(data[1..3].try_into().unwrap())),
                3,
            ),
            255 => (
                u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize,
                5,
            ),
        };
        let value = &data[offset..];
        assert_eq!(value.len(), length, "invalid redb byte vector length");
        value
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a &'b [u8]) -> Vec<u8>
    where
        Self: 'a,
        Self: 'b,
    {
        let mut encoded = Vec::with_capacity(value.len() + 5);
        match value.len() {
            0..=253 => encoded.push(value.len() as u8),
            254..=65535 => {
                encoded.push(254);
                encoded.extend_from_slice(&(value.len() as u16).to_le_bytes());
            }
            _ => {
                let length = u32::try_from(value.len()).expect("redb byte vector exceeds u32");
                encoded.push(255);
                encoded.extend_from_slice(&length.to_le_bytes());
            }
        }
        encoded.extend_from_slice(value);
        encoded
    }

    fn type_name() -> TypeName {
        <Vec<u8> as Value>::type_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_matches_legacy_vectors_at_every_length_boundary() {
        assert_eq!(ByteSlice::type_name(), <Vec<u8> as Value>::type_name());
        assert_eq!(ByteSlice::fixed_width(), <Vec<u8> as Value>::fixed_width());
        for length in [0, 1, 252, 253, 254, 255, 65534, 65535, 65536, 262144] {
            let value = (0..length).map(|i| i as u8).collect::<Vec<_>>();
            let legacy = <Vec<u8> as Value>::as_bytes(&value);
            let encoded = ByteSlice::as_bytes(&value.as_slice());
            assert_eq!(legacy, encoded);
            assert_eq!(<Vec<u8> as Value>::from_bytes(&encoded), value);
            let borrowed = ByteSlice::from_bytes(&legacy);
            assert_eq!(borrowed, value);
            assert_eq!(borrowed.as_ptr(), legacy[legacy.len() - length..].as_ptr());
        }
    }
}
