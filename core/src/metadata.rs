//! Common metadata exchanged between server and client

use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf, time::SystemTime};

use crate::{
    chunks::ChunkInfo, feed::{Feed, FeedName}, item::{ItemFormat, ItemName}, utils::serde::bitcode::BitcodeSerializable, version::Version
};

/// Serializable Server Metadata to be used by server and clients
#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
pub struct ServerMetadata {
    // TODO
    // server version
    pub version: Version,
    // Feed map
    pub feeds: HashMap<FeedName, Feed>,
    // Item map
    pub items: HashMap<ItemName, ItemMetadata>,
}

impl<'a> BitcodeSerializable<'a, ServerMetadata> for ServerMetadata {}

/// A compact subset of the fields in an Item
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct ItemMetadata {
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
    /// Creation SystemTime
    pub created: SystemTime,
    /// Last time the item has been updated
    pub updated: SystemTime,
    /// Version used to create the Item (same as the output of env!("CARGO_PKG_VERSION") on the creator.
    pub created_by: String,
    /// format used
    pub format: ItemFormat,
    //signature: Signature,
}

impl<'a> BitcodeSerializable<'a, ItemMetadata> for ItemMetadata {}

//Will be used in future to handle server-side tracking of clients for p2p distribution
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientsMetadata {
    pub feed_subscriptions: HashMap<FeedName, Feed>,
    pub item_subscriptions: HashMap<ItemName, ItemMetadata>,
}
