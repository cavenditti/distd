//! Common metadata exchanged between server and client

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf, time::SystemTime};

use crate::{
    chunks::ChunkInfo,
    feed::{Feed, Name as FeedName},
    item::{Format as ItemFormat, Name as ItemName},
    utils::serde::BitcodeSerializable,
    version::Version,
};

/// Serializable Server Metadata to be used by server and clients
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
pub struct Server {
    // TODO
    // server version
    pub version: Version,
    // Feed map
    pub feeds: HashMap<FeedName, Feed>,
    // Item map
    pub items: HashMap<PathBuf, Item>,
}

impl BitcodeSerializable<'_, Server> for Server {}

/// A compact subset of the fields in an Item
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Item {
    /// Name of the Item
    pub name: ItemName,
    /// Optional description, a generic String
    pub description: Option<String>,
    /// Incremental number of the file revision
    pub revision: u32,
    /// Path of the file (it may change among revisions?)
    pub path: PathBuf,
    /// BLAKE3 root hash of the file
    pub root: ChunkInfo,
    /// Creation `SystemTime`
    pub created: SystemTime,
    /// Last time the item has been updated
    pub updated: SystemTime,
    /// Version used to create the Item (same as the output of `env!("CARGO_PKG_VERSION`") on the creator.
    pub created_by: String,
    /// format used
    pub format: ItemFormat,
    //signature: Signature,
}

impl std::hash::Hash for Item {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.root.hash.hash(state);
        self.path.hash(state);
    }
}

impl Item {
    #[must_use]
    pub fn size(&self) -> u64 {
        self.root.size
    }
}

impl BitcodeSerializable<'_, Item> for Item {}

//Will be used in future to handle server-side tracking of clients for p2p distribution
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Clients {
    pub feed_subscriptions: HashMap<FeedName, Feed>,
    pub item_subscriptions: HashMap<ItemName, Item>,
}

#[cfg(test)]
mod tests {
    use crate::hash::Hash;

    use super::*;

    #[test]
    fn item() {
        let _ = Item {
            name: "An item".to_string(),
            description: Some("A description".to_string()),
            revision: 0,
            path: PathBuf::from("path/to/item"),
            root: ChunkInfo {
                hash: Hash::from_bytes([0; 32]),
                size: 0,
            },
            created: SystemTime::now(),
            updated: SystemTime::now(),
            created_by: "distd".to_string(),
            format: ItemFormat::V1,
        };
    }

    #[test]
    fn item_comparison() {
        let item = Item {
            name: "An item".to_string(),
            description: Some("A description".to_string()),
            revision: 0,
            path: PathBuf::from("path/to/item"),
            root: ChunkInfo {
                hash: Hash::from_bytes([0; 32]),
                size: 0,
            },
            created: SystemTime::now(),
            updated: SystemTime::now(),
            created_by: "distd".to_string(),
            format: ItemFormat::V1,
        };
        let item2 = item.clone();
        assert_eq!(item, item2);

        let item3 = Item {
            name: "Another item".to_string(),
            description: Some("A description".to_string()),
            revision: 0,
            path: PathBuf::from("path/to/item"),
            root: ChunkInfo {
                hash: Hash::from_bytes([0; 32]),
                size: 0,
            },
            created: SystemTime::now(),
            updated: SystemTime::now(),
            created_by: "distd".to_string(),
            format: ItemFormat::V1,
        };
        assert_ne!(item, item3);

    }

}
