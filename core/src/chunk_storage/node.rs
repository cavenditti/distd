use std::collections::HashMap;
use std::fmt::Display;
use std::{collections::HashSet, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::chunks::{ChunkInfo, CHUNK_SIZE};
use crate::hash::Hash;
use crate::utils::serde::nodes::{deserialize_arc_node, serialize_arc_node};

/// Arc reference to a raw byte chunk
pub type ArcChunk = Arc<Vec<u8>>;

/// This is the internal representation of the hash-tree
/// As it contains in-memory references, it is not meant to be serialized
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Node {
    Parent {
        hash: Hash,
        size: u64,
        #[serde(
            serialize_with = "serialize_arc_node",
            deserialize_with = "deserialize_arc_node"
        )]
        left: Arc<Node>,
        #[serde(
            serialize_with = "serialize_arc_node",
            deserialize_with = "deserialize_arc_node"
        )]
        right: Arc<Node>,
    },
    Stored {
        hash: Hash,
        data: ArcChunk,
    },

    /// Node skipped in serialization
    Skipped {
        hash: Hash,
        size: u64,
    },
}

/// Depth-first Iterator over the references of the chunks in the tree
///
/// This is the easy way of doing it, not the best one. expecially for large trees probably
// TODO do this in a better way
struct NodeIterator {
    stack: Vec<Arc<Node>>,
}

impl NodeIterator {
    fn new(node: Arc<Node>) -> Self {
        #[inline(always)]
        fn push_children(node: Arc<Node>, stack: &mut Vec<Arc<Node>>) {
            match node.clone().as_ref() {
                &Node::Stored { .. } | &Node::Skipped { .. } => {
                    // We're at a leaf, just return it
                    stack.push(node)
                }
                Node::Parent { left, right, .. } => {
                    // in this case we keep descending, first pushed get returned last
                    stack.push(node);
                    push_children(right.clone(), stack);
                    push_children(left.clone(), stack);
                }
            }
        }

        let mut stack = Vec::with_capacity((2 * node.size()) as usize / CHUNK_SIZE); // The very dumb heuristic™

        push_children(node, &mut stack);
        Self { stack }
    }
}

impl Iterator for NodeIterator {
    type Item = Arc<Node>;

    fn next(&mut self) -> Option<Self::Item> {
        self.stack.pop()
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let h_str = self.hash().to_string()[..8].to_string() + "…";
        match self {
            Self::Parent {
                size, left, right, ..
            } => {
                write!(
                    f,
                    "HashTreeNode::Parent {{ {h_str}, <LEFT: {left}, RIGHT: {right}>, {size}B }}",
                )
            }
            Self::Stored { data, .. } => {
                write!(f, "HashTreeNode::Stored {{ {h_str}, {}B }}", data.len(),)
            }
            Self::Skipped { size, .. } => {
                write!(f, "HashTreeNode::Skipped {{ {h_str}, {size}B  }}")
            }
        }
    }
}

impl Node {
    #[must_use]
    #[inline(always)]
    pub fn hash(&self) -> &Hash {
        match self {
            Self::Stored { hash, .. } | Self::Parent { hash, .. } | Self::Skipped { hash, .. } => {
                hash
            }
        }
    }

    /// Compute sum size in bytes of all descending chunks
    #[must_use]
    #[inline(always)]
    pub fn size(&self) -> u64 {
        match self {
            Self::Stored { data, .. } => data.len() as u64,
            Self::Parent { size, .. } | Self::Skipped { size, .. } => *size,
        }
    }

    #[must_use]
    pub fn chunk_info(&self) -> ChunkInfo {
        match self {
            Self::Stored { hash, data } => ChunkInfo {
                hash: *hash,
                size: data.len() as u64,
            },
            Self::Skipped { hash, size, .. } | Self::Parent { hash, size, .. } => ChunkInfo {
                hash: *hash,
                size: *size,
            },
        }
    }

    /// Get contained data, returns None if is not Stored
    #[must_use]
    pub fn stored_data(&self) -> Option<ArcChunk> {
        match self {
            Self::Stored { data, .. } => Some(data.clone()),
            Self::Parent { .. } | Self::Skipped { .. } => None,
        }
    }

    /// Get contained data, returns None if is not Parent
    #[must_use]
    pub fn children(&self) -> Option<(&Arc<Node>, &Arc<Node>)> {
        match self {
            Self::Parent { left, right, .. } => Some((left, right)),
            Self::Stored { .. } | Self::Skipped { .. } => None,
        }
    }

    /// Get a view on contained data, recursing across all children
    #[must_use]
    pub fn data(&self) -> Option<Vec<ArcChunk>> {
        match self {
            Self::Stored { data, .. } => Some(vec![data.clone()]),
            Self::Parent { left, right, .. } => {
                let mut left_vec = left.data()?;
                left_vec.extend(right.data()?);
                Some(left_vec)
            }
            Self::Skipped { .. } => None, // Fail on any Skipped
        }
    }

    /// Get contained data, recursing across all children
    /// This method may be slow and produce (copying) a large result, pay attention when using it
    #[must_use]
    pub fn clone_data(&self) -> Vec<u8> {
        match self {
            Self::Stored { data, .. } => (*data.clone()).clone(),
            Self::Parent { left, right, .. } => {
                let mut left_vec = left.clone_data();
                left_vec.extend(right.clone_data());
                left_vec
            }
            Self::Skipped { .. } => vec![], // FIXME should fail
        }
    }

    /// Get flatten representation of `Stored` hashes, eventually repeating hashes
    #[must_use]
    pub fn flatten(&self) -> Vec<Hash> {
        match self {
            Self::Stored { hash, .. } => {
                vec![*hash]
            }
            Self::Parent { left, right, .. } => {
                let mut left_vec = left.flatten();
                left_vec.extend(right.flatten());
                left_vec
            }
            Self::Skipped { .. } => vec![], // FIXME should fail
        }
    }

    /// Get all unique `Stored` hashes referenced by the (sub-)tree
    #[must_use]
    pub fn hashes(&self) -> HashSet<Hash> {
        match self {
            Self::Stored { hash, .. } => HashSet::from([*hash]),
            Self::Parent { left, right, .. } => {
                let left_vec = left.hashes();
                left_vec.union(&right.hashes()).copied().collect()
            }
            Self::Skipped { .. } => HashSet::new(),
        }
    }

    /// Get all unique hashes (`Stored`, `Parent` or `Skipped`) referenced by the (sub-)tree
    #[must_use]
    pub fn all_hashes(&self) -> HashSet<Hash> {
        match self {
            Self::Stored { hash, .. } => HashSet::from([*hash]),
            Self::Parent {
                hash, left, right, ..
            } => {
                let mut left_vec = left.all_hashes();
                left_vec.insert(*hash);
                left_vec.union(&right.all_hashes()).copied().collect()
            }
            Self::Skipped { hash, .. } => HashSet::from([*hash]),
        }
    }

    /// Get all unique `Stored` hashes referenced by the (sub-)tree
    #[must_use]
    pub fn hashes_with_sizes(&self) -> HashSet<ChunkInfo> {
        match self {
            Self::Stored { hash, .. } => HashSet::from([ChunkInfo {
                size: self.size(),
                hash: *hash,
            }]),
            Self::Parent { left, right, .. } => {
                let left_vec = left.hashes_with_sizes();
                left_vec
                    .union(&right.hashes_with_sizes())
                    .copied()
                    .collect()
            }
            Self::Skipped { .. } => HashSet::new(),
        }
    }

    /// Get all unique `Stored` hashes referenced by the (sub-)tree
    #[must_use]
    pub fn all_hashes_with_sizes(&self) -> HashSet<ChunkInfo> {
        match self {
            Self::Stored { hash, .. } => HashSet::from([ChunkInfo {
                size: self.size(),
                hash: *hash,
            }]),
            Self::Parent {
                hash,
                left,
                right,
                size,
            } => {
                let mut left_vec = left.hashes_with_sizes();
                left_vec.insert(ChunkInfo {
                    size: *size,
                    hash: *hash,
                });
                left_vec
                    .union(&right.hashes_with_sizes())
                    .copied()
                    .collect()
            }
            Self::Skipped { .. } => HashSet::new(),
        }
    }

    /// Get flatten representation of `Stored` hashes with sizes, eventually repeating hashes
    #[must_use]
    pub fn flatten_with_sizes(&self) -> Vec<ChunkInfo> {
        match self {
            Self::Stored { hash, .. } => {
                vec![ChunkInfo {
                    size: self.size(),
                    hash: *hash,
                }]
            }
            Self::Parent { left, right, .. } => {
                let mut left_vec = left.flatten_with_sizes();
                left_vec.extend(right.flatten_with_sizes());
                left_vec
            }
            Self::Skipped { .. } => vec![],
        }
    }

    /// Flatten the tree into an iterator on chunks
    ///
    /// This is a recursive function that returns an iterator on the chunks of the tree
    ///
    /// # Returns
    /// An iterator on the chunks of the tree
    ///
    /// # Panics
    /// If the tree contains a `Skipped` node
    #[must_use]
    pub fn flatten_iter(&self) -> Box<dyn Iterator<Item = Arc<Vec<u8>>>> {
        match self {
            Self::Stored { data, .. } => Box::new([data.clone()].into_iter()),
            Self::Parent { left, right, .. } => {
                Box::new(left.flatten_iter().chain(right.flatten_iter()))
            }
            Self::Skipped { .. } => Box::new([].into_iter()), //FIXME should fail
        }
    }

    fn is_complete(&self) -> bool {
        match self {
            Self::Stored { .. } => true,
            Self::Skipped { .. } => false,
            Self::Parent { left, right, .. } => left.is_complete() && right.is_complete(),
        }
    }

    /// Get all unique hashes (`Stored` or `Parent`) referenced by the (sub-)tree, as an HashMap
    pub fn hash_map(self: Arc<Node>) -> HashMap<Hash, Arc<Node>> {
        match self.as_ref() {
            &Node::Stored { hash, .. } | &Node::Skipped { hash, .. } => {
                HashMap::from([(hash, self.clone())])
            }
            Node::Parent {
                hash, left, right, ..
            } => {
                let mut left_map = left.clone().hash_map();
                left_map.extend(right.clone().hash_map());
                left_map.insert(*hash, self.clone());
                left_map
            }
        }
    }

    /// Get all unique hashes (`Stored` or `Parent`) referenced by the (sub-)tree, as a HashMap
    pub fn find_diff(self: Arc<Node>, hashes: &[Hash]) -> impl Iterator<Item = Arc<Node>> {
        // prepare hash map
        let mut node_map = self.clone().hash_map();

        // Remove hashes from node_map keys
        for h in hashes {
            node_map.remove(h);
        }

        // Do a DFS-iteration on the tree from the root:
        // if node is not in Map:
        //  return Skipped
        // else:
        //  return Node::from(node)
        // O(hashes) + O(tree-nodes) * ~O(1) -> ~O(tree-nodes) (it's actually O(n log(n)), but hash maps pump those log factors down down)
        NodeIterator::new(self).map(move |x| {
            node_map
                .get(x.hash())
                .cloned()
                .unwrap_or(Arc::new(Node::Skipped {
                    hash: *x.hash(),
                    size: x.size(),
                }))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        chunks::CHUNK_SIZE,
        hash::{hash, merge_hashes},
    };
    use rand::RngCore;

    use super::Node;

    #[test]
    fn test_flatten() {
        const L: usize = 2000;
        let b1 = Arc::new(vec![0u8; L]);
        let b2 = Arc::new(vec![1u8; L]);
        let l1 = b1.len();
        let l2 = b2.len();
        let l = Node::Stored {
            hash: hash(&b1),
            data: b1,
        };
        let r = Node::Stored {
            hash: hash(&b2),
            data: b2,
        };
        let n = Node::Parent {
            hash: merge_hashes(l.hash(), r.hash()),
            size: l.size() + r.size(),
            left: Arc::new(l),
            right: Arc::new(r),
        };

        let flat: Vec<u8> = n.flatten_iter().flat_map(|x| (*x).clone()).collect();

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
        let node = Node::Stored {
            hash: h,
            data: Arc::new(data),
        };
        let diff = Arc::new(node).find_diff(&[h]).last().unwrap();
        assert!(matches!(diff.as_ref(), &Node::Skipped { .. }));
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
        let node = Node::Parent {
            hash: h,
            size: data_size as u64,
            left: Arc::new(Node::Parent {
                hash: h12,
                size: (CHUNK_SIZE * 2) as u64,
                left: Arc::new(Node::Stored {
                    hash: h1,
                    data: Arc::new(data[..CHUNK_SIZE].to_vec()),
                }),
                right: Arc::new(Node::Stored {
                    hash: h2,
                    data: Arc::new(data[CHUNK_SIZE..CHUNK_SIZE * 2].to_vec()),
                }),
            }),
            right: Arc::new(Node::Parent {
                hash: h34,
                size: (CHUNK_SIZE + 4) as u64,
                left: Arc::new(Node::Stored {
                    hash: h3,
                    data: Arc::new(data[CHUNK_SIZE * 2..CHUNK_SIZE * 3].to_vec()),
                }),
                right: Arc::new(Node::Stored {
                    hash: h4,
                    data: Arc::new(data[CHUNK_SIZE * 3..].to_vec()),
                }),
            }),
        };

        let node = Arc::new(node);

        // We now proceed to test all possible combinations of provided hashes to check the results are the expected ones.

        // Single elements
        for comb in [&[h1], &[h2], &[h3], &[h4]] {
            // last is always root
            let diff = node.clone().find_diff(comb).last().unwrap();
            assert!(matches!(diff.as_ref(), &Node::Parent { .. }));
            assert_eq!(diff.hash(), &h);
            assert_eq!(diff.size(), data_size as u64);

            let (left, right) = diff.children().unwrap();
            assert!(matches!(left.as_ref(), &Node::Parent { .. }));
            assert!(matches!(right.as_ref(), &Node::Parent { .. }));
        }

        // Disjoint pairs, i.e. pairs not constituting a sub-tree of their own
        for comb in [&[h1, h3], &[h2, h3], &[h1, h4], &[h2, h4]] {
            let diff = node.clone().find_diff(comb).last().unwrap();
            assert!(matches!(diff.as_ref(), &Node::Parent { .. }));
            assert_eq!(diff.hash(), &h);
            assert_eq!(diff.size(), data_size as u64);

            let (left, right) = diff.children().unwrap();
            assert!(matches!(left.as_ref(), &Node::Parent { .. }));
            assert!(matches!(right.as_ref(), &Node::Parent { .. }));
        }

        // Sub-tree pairs
        for comb in [&[h1, h2], &[h3, h4]] {
            let diff = node.clone().find_diff(comb).last().unwrap();
            assert!(matches!(diff.as_ref(), &Node::Parent { .. }));
            assert_eq!(diff.hash(), &h);
            assert_eq!(diff.size(), data_size as u64);

            let (left, right) = diff.children().unwrap();
            println!("{}", left);
            println!("{}", right);
            assert!(
                (matches!(left.as_ref(), &Node::Parent { .. })
                    && matches!(right.as_ref(), &Node::Skipped { .. }))
                    || (matches!(left.as_ref(), &Node::Skipped { .. })
                        && matches!(right.as_ref(), &Node::Parent { .. }))
            );
        }

        // Three elements
        for comb in [&[h1, h2, h3], &[h1, h2, h4], &[h1, h3, h4], &[h2, h3, h4]] {
            let diff = node.clone().find_diff(comb).last().unwrap();
            assert!(matches!(diff.as_ref(), &Node::Parent { .. }));
            assert_eq!(diff.hash(), &h);
            assert_eq!(diff.size(), data_size as u64);

            let (left, right) = diff.children().unwrap();
            assert!(
                (matches!(left.as_ref(), &Node::Parent { .. })
                    && matches!(right.as_ref(), &Node::Skipped { .. }))
                    || (matches!(left.as_ref(), &Node::Skipped { .. })
                        && matches!(right.as_ref(), &Node::Parent { .. }))
            );
        }

        let diff = node.clone().find_diff(&[h]).last().unwrap();
        assert!(matches!(diff.as_ref(), &Node::Skipped { .. }));
        assert_eq!(diff.hash(), &h);
        assert_eq!(diff.size(), data_size as u64);

        let diff = node.clone().find_diff(&[h1, h2, h3, h4]).last().unwrap();
        assert!(matches!(diff.as_ref(), &Node::Skipped { .. }));
        assert_eq!(diff.hash(), &h);
        assert_eq!(diff.size(), data_size as u64);
    }
}
