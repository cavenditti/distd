//! Common metadata exchanged between server and client

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{item::ItemName, feed::FeedName, version::Version};

/// Serializable Server Metadata to be used by server and clients
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ServerMetadata {
    // TODO
    // server version
    pub version: Version,
    // Feed map
    pub feeds: HashMap<FeedName, FeedMetadata>,
    // Item map
    pub items: HashMap<ItemName, ItemMetadata>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FeedMetadata {}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ItemMetadata {}

//Will be used in future to handle server-side tracking of clients for p2p distribution
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientsMetadata {
    pub feed_subscriptions: HashMap<FeedName, FeedMetadata>,
    pub item_subscriptions: HashMap<ItemName, ItemMetadata>,
}
