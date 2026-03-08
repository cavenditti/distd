use std::{sync::Arc, time::Duration};

use tokio_stream::{Stream, StreamExt};

use crate::utils::stream::{BatchingStream, DeBatchingStream};

use super::Node;

type NodeBatchingStream<S, Fn> = tokio_stream::adapters::Map<BatchingStream<S>, Fn>;

/// Create a sender stream that serializes nodes into bitcode
///
/// The sender stream will batch nodes into `batch_size`, at most every `duration`.
/// The serialization is done using the bitcode format.
///
/// # Errors
///
/// Serialization errors are logged and the batch is skipped (yielding no output for that batch).
pub fn sender<S>(
    stream: S,
    batch_size: usize,
    duration: Duration,
) -> NodeBatchingStream<S, impl FnMut(<BatchingStream<S> as Stream>::Item) -> Vec<u8>>
where
    S: Stream<Item = Arc<Node>>,
    BatchingStream<S>: StreamExt,
    <BatchingStream<S> as Stream>::Item: serde::Serialize,
{
    let s = BatchingStream::new(stream, batch_size, duration);
    s.map(|x| {
        bitcode::serialize(&x)
            .inspect_err(|e| tracing::error!("Cannot serialize node batch: {e}"))
            .unwrap_or_default()
    })
}

/// Create a receiver stream that deserializes nodes from bitcode
///
/// The receiver stream will de-batch nodes into `batch_size`, at most every `duration`.
///
/// # Errors
///
/// Deserialization errors are logged and the malformed batch is dropped (skipped).
pub fn receiver<S>(
    stream: S,
    batch_size: usize,
    duration: Duration,
) -> DeBatchingStream<Node, impl Stream<Item = Vec<Node>>>
where
    S: Stream<Item = Vec<u8>>,
{
    let stream = stream
        .map(|x| -> Option<Vec<Node>> {
            bitcode::deserialize(&x)
                .inspect_err(|e| tracing::error!("Cannot deserialize node batch: {e}"))
                .ok()
        })
        .filter_map(|x| x);
    DeBatchingStream::new(stream, batch_size, duration)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::chunk_storage::Node;
    use crate::hash::{hash as do_hash, merge_hashes};

    use super::*;

    async fn batched_node_roundtrip(nodes: &Vec<Node>) {
        let nodes: Vec<Arc<Node>> = nodes.iter().cloned().map(Arc::new).collect();
        let stream = tokio_stream::iter(nodes.clone());
        let sender = sender(stream, 32, Duration::new(4, 0));
        let mut receiver = receiver(sender, 32, Duration::new(4, 0));

        let mut count = 0;
        while let Some(node) = receiver.next().await {
            // Special handling for parents because children are Arc and get replaced with a Skipped Node
            match node {
                Node::Parent {
                    hash,
                    size,
                    left,
                    right,
                } => {
                    assert!(matches!(left.as_ref(), &Node::Skipped { .. }));
                    assert!(matches!(right.as_ref(), &Node::Skipped { .. }));

                    let original = nodes[count].clone();
                    let (o_left, o_right) = original.children().unwrap();

                    assert_eq!(&hash, original.hash());
                    assert_eq!(size, original.size());

                    assert_eq!(left.hash(), o_left.hash());
                    assert_eq!(right.hash(), o_right.hash());
                    assert_eq!(left.size(), o_left.size());
                    assert_eq!(right.size(), o_right.size());
                }
                n => assert_eq!(&n, nodes[count].as_ref()),
            }
            count += 1;
        }

        assert_eq!(count, nodes.len());
    }

    #[tokio::test]
    async fn batched_node_roundtrip_1() {
        let nodes = vec![Node::Stored {
            hash: do_hash(b""),
            data: Arc::default(),
        }];

        batched_node_roundtrip(&nodes).await;
    }

    #[tokio::test]
    async fn batched_node_roundtrip_some() {
        let l = Node::Stored {
            hash: do_hash(b""),
            data: Arc::default(),
        };
        let r = Node::Stored {
            hash: do_hash(b"somedata"),
            data: Arc::new(b"somedata".into()),
        };
        let nodes = vec![
            l.clone(),
            r.clone(),
            Node::Parent {
                hash: merge_hashes(l.hash(), r.hash()),
                size: l.size() + r.size(),
                left: Arc::new(l),
                right: Arc::new(r),
            },
            Node::Skipped {
                hash: do_hash(b""),
                size: 0,
            },
        ];

        batched_node_roundtrip(&nodes).await;
    }
}
