use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::item::Item;
use crate::unique_name::UniqueName;

pub type Name = UniqueName;

/// A `Feed` is a collection of items
///
/// It is identified by a unique name
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Feed {
    /// Name of the feed
    pub name: Name,

    /// Paths of items in the feed
    pub paths: BTreeMap<PathBuf, Item>,
}

impl Feed {
    #[must_use] pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            paths: BTreeMap::new(),
        }
    }
}

impl PartialEq for Feed {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.paths.len() == other.paths.len()
            && self
                .paths
                .iter()
                .zip(other.paths.iter())
                .all(|((x_path, x_item), (y_path, y_item))| x_path == y_path && x_item == y_item)
    }
}
impl Eq for Feed {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feed() {
        let feed = Feed::new("A feed");
        assert_eq!("A feed", feed.name);
    }
}
