use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::metadata::Item;
use crate::unique_name::UniqueName;
use crate::msgpack::MsgPackSerializable;

pub type FeedName = UniqueName;

#[derive(Debug, Serialize, Deserialize)]
pub struct Feed {
    pub name: FeedName,
    pub paths: BTreeMap<PathBuf, Item>,
}

impl Feed {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            paths: BTreeMap::new(),
        }
    }
}

impl<'a> MsgPackSerializable<'a, Feed> for Feed {}
