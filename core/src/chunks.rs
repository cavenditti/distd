//! common chunks and hash-tree data structs


use serde::{Deserialize, Serialize};

use crate::hash::Hash;

/// Chunk size in bytes
/// It may useful to increase this in order to print hash tree when debugging
//pub const CHUNK_SIZE: usize = blake3::guts::CHUNK_LEN;
pub const CHUNK_SIZE: usize = 512 * 1024;

/// Owned chunk
pub type OwnedChunk = Vec<u8>;

//pub type Size = u64;

/// Node in an hash-tree
/// It may be a parent node with two children or a leaf node with owned data
pub trait HashTreeNode {
    /// Get hash of node
    fn hash(&self) -> &Hash;

    /// Compute sum size in bytes of all descending chunks
    fn size(&self) -> u64;

    /// Get contained data, returns None if is not Stored
    fn stored_data(&self) -> Option<&OwnedChunk>;

    /// Get mutable contained data, returns None if is not Stored
    fn stored_data_mut(&mut self) -> Option<&mut OwnedChunk>;

    /// Get owned contained data, returns None if is not `Stored`
    fn stored_data_owned(self) -> Option<OwnedChunk>;

    /// Get contained data, returns None if is not Parent
    fn children(&self) -> Option<(&Self, &Self)>;

    /// Get diff sub-tree: required tree to reconstruct current node if one has the `hashes`
    #[must_use]
    fn find_diff(&self, hashes: &[Hash]) -> Self;

    /// Flatten the tree into an iterator on chunks
    fn flatten_iter(self) -> Box<dyn Iterator<Item = Vec<u8>>>;

    /// Wheter the subtree has any missing node
    ///
    /// A default implementation si provided to skip this if the type cannot have missing nodes
    fn is_complete(&self) -> bool {
        true
    }
}

/// Seralizable view of an hash tree node, only contains size and hash
#[derive(Debug, Clone, Copy, Serialize, Deserialize, std::hash::Hash, PartialEq, Eq)]
pub struct ChunkInfo {
    // progressive unique id provided by the storage ??
    //pub id: u64,
    // Chunk size
    pub size: u64,
    // Chunk hash
    pub hash: Hash,
}

impl ChunkInfo {
    #[allow(dead_code)] // TODO check if it's needed
    fn is_leaf(&self) -> bool {
        //self.children.is_none()
        self.size == CHUNK_SIZE as u64
    }
}

// TODO move to StoredChunkRef
/*
#[cfg(test)]
mod tests {
    use crate::{
        chunks::CHUNK_SIZE,
        hash::{hash, merge_hashes},
    };
    use rand::RngCore;

    use super::{flatten, HashTreeNode, OwnedHashTreeNode};

    #[test]
    fn test_flatten() {
        const L: usize = 2000;
        let b1 = vec![0; L];
        let b2 = vec![1; L];
        let l1 = b1.len();
        let l2 = b2.len();
        let n1 = OwnedHashTreeNode::Stored {
            hash: hash(b""),
            data: b1,
        };
        let n2 = OwnedHashTreeNode::Stored {
            hash: hash(b""),
            data: b2,
        };
        let flat = flatten(vec![n1, n2]).unwrap();

        assert_eq!(flat.len(), l1 + l2);

        for v in &flat[..L] {
            assert_eq!(*v, 0);
        }

        for v in &flat[L..] {
            assert_eq!(*v, 1);
        }
    }

    #[test]
    fn test_node_find_diff_noop() {
        let data = vec![1u8; 12_000];
        let h = hash(&data);
        let node = OwnedHashTreeNode::Stored { hash: h, data };
        let diff = node.find_diff(&[h]);
        assert!(matches!(diff, OwnedHashTreeNode::Skipped { .. }));
        assert_eq!(diff.hash(), &h);
        assert_eq!(diff.size(), 12_000);
    }

    #[test]
    fn test_node_find_diff() {
        let data_size = CHUNK_SIZE * 3 + 4;
        let mut data = vec![0u8; data_size];
        rand::rngs::OsRng::default().fill_bytes(&mut data);

        let h1 = hash(&data[..CHUNK_SIZE]);
        let h2 = hash(&data[CHUNK_SIZE..CHUNK_SIZE * 2]);
        let h12 = merge_hashes(&h1, &h2);
        let h3 = hash(&data[CHUNK_SIZE * 2..CHUNK_SIZE * 3]);
        let h4 = hash(&data[CHUNK_SIZE * 3..]);
        let h34 = merge_hashes(&h3, &h4);
        let h = merge_hashes(&h12, &h34);

        // Actually testing the test here
        assert_eq!(h, hash(&data));

        // Manually create nodes
        let node = OwnedHashTreeNode::Parent {
            hash: h,
            size: data_size as u32,
            left: Box::new(OwnedHashTreeNode::Parent {
                hash: h12,
                size: (CHUNK_SIZE * 2) as u32,
                left: Box::new(OwnedHashTreeNode::Stored {
                    hash: h1,
                    data: data[..CHUNK_SIZE].to_vec(),
                }),
                right: Box::new(OwnedHashTreeNode::Stored {
                    hash: h2,
                    data: data[CHUNK_SIZE..CHUNK_SIZE * 2].to_vec(),
                }),
            }),
            right: Box::new(OwnedHashTreeNode::Parent {
                hash: h34,
                size: (CHUNK_SIZE + 4) as u32,
                left: Box::new(OwnedHashTreeNode::Stored {
                    hash: h3,
                    data: data[CHUNK_SIZE * 2..CHUNK_SIZE * 3].to_vec(),
                }),
                right: Box::new(OwnedHashTreeNode::Stored {
                    hash: h4,
                    data: data[CHUNK_SIZE * 3..].to_vec(),
                }),
            }),
        };

        // We now proceed to test all possible combinations of provided hashes to check the results are the expected ones.

        // Single elements
        for comb in [&[h1], &[h2], &[h3], &[h4]] {
            let diff = node.find_diff(comb);
            assert!(matches!(diff, OwnedHashTreeNode::Parent { .. }));
            assert_eq!(diff.hash(), &h);
            assert_eq!(diff.size(), data_size as u32);

            let (left, right) = diff.children().unwrap();
            assert!(matches!(left, OwnedHashTreeNode::Parent { .. }));
            assert!(matches!(right, OwnedHashTreeNode::Parent { .. }));
        }

        // Disjoint pairs, i.e. pairs not constituting a sub-tree of their own
        for comb in [&[h1, h3], &[h2, h3], &[h1, h4], &[h2, h4]] {
            let diff = node.find_diff(comb);
            assert!(matches!(diff, OwnedHashTreeNode::Parent { .. }));
            assert_eq!(diff.hash(), &h);
            assert_eq!(diff.size(), data_size as u32);

            let (left, right) = diff.children().unwrap();
            assert!(matches!(left, OwnedHashTreeNode::Parent { .. }));
            assert!(matches!(right, OwnedHashTreeNode::Parent { .. }));
        }

        // Sub-tree pairs
        for comb in [&[h1, h2], &[h3, h4]] {
            let diff = node.find_diff(comb);
            assert!(matches!(diff, OwnedHashTreeNode::Parent { .. }));
            assert_eq!(diff.hash(), &h);
            assert_eq!(diff.size(), data_size as u32);

            let (left, right) = diff.children().unwrap();
            assert!(
                (matches!(left, OwnedHashTreeNode::Parent { .. })
                    && matches!(right, OwnedHashTreeNode::Skipped { .. }))
                    || (matches!(left, OwnedHashTreeNode::Skipped { .. })
                        && matches!(right, OwnedHashTreeNode::Parent { .. }))
            );
        }

        // Three elements
        for comb in [&[h1, h2, h3], &[h1, h2, h4], &[h1, h3, h4], &[h2, h3, h4]] {
            let diff = node.find_diff(comb);
            assert!(matches!(diff, OwnedHashTreeNode::Parent { .. }));
            assert_eq!(diff.hash(), &h);
            assert_eq!(diff.size(), data_size as u32);

            let (left, right) = diff.children().unwrap();
            assert!(
                (matches!(left, OwnedHashTreeNode::Parent { .. })
                    && matches!(right, OwnedHashTreeNode::Skipped { .. }))
                    || (matches!(left, OwnedHashTreeNode::Skipped { .. })
                        && matches!(right, OwnedHashTreeNode::Parent { .. }))
            );
        }

        let diff = node.find_diff(&[h]);
        assert!(matches!(diff, OwnedHashTreeNode::Skipped { .. }));
        assert_eq!(diff.hash(), &h);
        assert_eq!(diff.size(), data_size as u32);

        let diff = node.find_diff(&[h1, h2, h3, h4]);
        assert!(matches!(diff, OwnedHashTreeNode::Skipped { .. }));
        assert_eq!(diff.hash(), &h);
        assert_eq!(diff.size(), data_size as u32);
    }
}
*/
