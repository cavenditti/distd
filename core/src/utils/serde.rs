//! Various serde-related utils
use crate::hash::Hash;
use serde::{de, Deserialize, Deserializer};
use std::{fmt, str::FromStr};

//pub mod msgpack;
pub mod bitcode;

pub use bitcode::Serializable as BitcodeSerializable;

/// Serde deserialization decorator to map empty Strings to None,
pub fn empty_string_as_none<'de, D, T>(de: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    T::Err: fmt::Display,
{
    let opt = Option::<String>::deserialize(de)?;
    match opt.as_deref() {
        None | Some("") => Ok(None),
        Some(s) => FromStr::from_str(s).map_err(de::Error::custom).map(Some),
    }
}

pub mod nodes {
    use std::fmt;
    use std::sync::Arc;

    use serde::de::Visitor;
    use serde::{de::SeqAccess, ser::SerializeStruct};

    use super::{de, Deserialize, Deserializer, Hash};
    use crate::chunk_storage::Node;

    pub fn serialize_arc_node<S>(v: &Arc<Node>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("Arc<Node>", 2)?;
        state.serialize_field("hash", v.hash())?;
        state.serialize_field("size", &v.size())?;
        state.end()
    }

    pub fn deserialize_arc_node<'de, D>(deserializer: D) -> Result<Arc<Node>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Hash,
            Size,
        }

        struct DurationVisitor;

        impl<'de> Visitor<'de> for DurationVisitor {
            type Value = Arc<Node>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct derived from Arc<Node>")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Arc<Node>, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let hash = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let size = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(Arc::new(Node::Skipped { hash, size }))
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut hash: Option<Hash> = None;
                let mut size: Option<u64> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Hash => {
                            if hash.is_some() {
                                return Err(de::Error::duplicate_field("hash"));
                            }
                            hash = map.next_value()?;
                        }
                        Field::Size => {
                            if size.is_some() {
                                return Err(de::Error::duplicate_field("size"));
                            }
                            size = map.next_value()?
                        }
                    }
                }
                let hash = hash.ok_or(de::Error::missing_field("hash"))?;
                let size = size.ok_or(de::Error::missing_field("size"))?;
                Ok(Arc::new(Node::Skipped { hash, size }))
            }
        }

        const FIELDS: &[&str] = &["hash", "size"];
        deserializer.deserialize_struct("Duration", FIELDS, DurationVisitor)
    }
}
