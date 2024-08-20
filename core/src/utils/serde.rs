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

pub mod hashes {
    use super::*;

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
}

pub mod opt_hash_struct {
    use super::*;

    pub fn serialize_opt_hash<S>(v: &Option<Hash>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            Option::<String>::serialize(&v.map(|x| x.to_string()), serializer)
        } else {
            Option::<[u8; 32]>::serialize(&v.map(|x| *x.as_bytes()), serializer)
        }
    }

    pub fn serialize_opt_2tuple_hash<S>(
        v: &Option<(Hash, Hash)>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            Option::<(String, String)>::serialize(
                &v.map(|(x, y)| (x.to_string(), y.to_string())),
                serializer,
            )
        } else {
            Option::<([u8; 32], [u8; 32])>::serialize(
                &v.map(|(x, y)| (*x.as_bytes(), *y.as_bytes())),
                serializer,
            )
        }
    }

    pub fn deserialize_opt_hash<'de, D>(deserializer: D) -> Result<Option<Hash>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        if deserializer.is_human_readable() {
            Option::<String>::deserialize(deserializer)
                .transpose()
                .map(|x| match x {
                    Ok(x) => Ok(Hash::from_str(&x).map_err(Error::custom)?),
                    Err(e) => Err(e),
                })
                .transpose()
        } else {
            Option::<[u8; 32]>::deserialize(deserializer).map(|x| x.map(|x| Hash::from_bytes(x)))
        }
    }

    pub fn deserialize_opt_2tuple_hash<'de, D>(
        deserializer: D,
    ) -> Result<Option<(Hash, Hash)>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        if deserializer.is_human_readable() {
            Option::<(String, String)>::deserialize(deserializer)
                .transpose()
                .map(|x| match x {
                    Ok((x, y)) => Ok((
                        Hash::from_str(&x).map_err(Error::custom)?,
                        Hash::from_str(&y).map_err(Error::custom)?,
                    )),
                    Err(e) => Err(e),
                })
                .transpose()
        } else {
            Option::<([u8; 32], [u8; 32])>::deserialize(deserializer)
                .map(|x| x.map(|(x, y)| (Hash::from_bytes(x), Hash::from_bytes(y))))
        }
    }
}
