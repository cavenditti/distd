//! Various serde-related utils
use blake3::Hash;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{fmt, str::FromStr};

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

pub fn serialize_hash<S>(v: &Hash, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if serializer.is_human_readable() {
        v.to_string().serialize(serializer)
    } else {
        v.as_bytes().serialize(serializer)
    }
}

pub fn deserialize_hash<'de, D>(deserializer: D) -> Result<Hash, D::Error>
where
    D: Deserializer<'de>,
{
    if deserializer.is_human_readable() {
        let s: String = Deserialize::deserialize(deserializer)?;
        blake3::Hash::from_str(&s).map_err(|e| de::Error::custom(e.to_string()))
    } else {
        let s: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(blake3::Hash::from_bytes(s))
    }
}

pub fn serialize_hash_vec<S>(v: &Vec<Hash>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    if serializer.is_human_readable() {
        v.into_iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .serialize(serializer)
    } else {
        v.into_iter()
            .map(|e| *e.as_bytes())
            .collect::<Vec<[u8; 32]>>()
            .serialize(serializer)
    }
}

pub fn deserialize_hash_vec<'de, D>(deserializer: D) -> Result<Vec<Hash>, D::Error>
where
    D: Deserializer<'de>,
{
    if deserializer.is_human_readable() {
        let s: Vec<String> = Deserialize::deserialize(deserializer)?;
        Ok(s.into_iter()
            .map(|e| blake3::Hash::from_str(&e))
            .flatten()
            .collect::<Vec<Hash>>())
    } else {
        let s: Vec<[u8; 32]> = Deserialize::deserialize(deserializer)?;
        Ok(s.into_iter()
            .map(|e| blake3::Hash::from_bytes(e))
            .collect::<Vec<Hash>>())
    }
}
