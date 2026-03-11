use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use tokio::time::{Duration, Instant, Sleep};
use tokio_stream::Stream;

/// A stream that batches items from an inner stream.
///
/// The stream will emit a batch of items when either the batch size is reached or the timeout
/// duration has elapsed since the last batch was emitted. If the inner stream completes and there
/// are still items in the buffer, the stream will emit a final batch with the remaining items.
///
/// Order of items in stream is preserved.
pub struct BatchingStream<S>
where
    S: Stream,
{
    stream: S,
    batch_size: usize,
    timeout: Duration,
    buffer: VecDeque<S::Item>, // TODO limit capacity
    last_emit: Instant,
    delay: Pin<Box<Sleep>>,
}

impl<S> BatchingStream<S>
where
    S: Stream,
{
    pub fn new(stream: S, batch_size: usize, timeout: Duration) -> Self {
        Self {
            stream,
            batch_size,
            timeout,
            buffer: VecDeque::default(),
            last_emit: Instant::now(),
            delay: Box::pin(tokio::time::sleep(timeout)),
        }
    }
}

impl<S> Stream for BatchingStream<S>
where
    S: Stream + Unpin,
    S::Item: Unpin,
{
    type Item = Vec<S::Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if (this.buffer.len() >= this.batch_size || this.last_emit.elapsed() >= this.timeout)
                && !this.buffer.is_empty()
            {
                let batch = std::mem::take(&mut this.buffer);
                this.last_emit = Instant::now();
                return Poll::Ready(Some(batch.into()));
            }

            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.buffer.push_back(item);
                }
                Poll::Ready(None) => {
                    return if this.buffer.is_empty() {
                        Poll::Ready(None)
                    } else {
                        let batch = std::mem::take(&mut this.buffer);
                        Poll::Ready(Some(batch.into()))
                    }
                }
                Poll::Pending => {
                    if this.buffer.is_empty() {
                        return Poll::Pending;
                    }
                    // Register a waker via Sleep so the executor re-polls after the timeout.
                    this.delay.as_mut().reset(this.last_emit + this.timeout);
                    if this.delay.as_mut().poll(cx).is_ready() {
                        let batch = std::mem::take(&mut this.buffer);
                        this.last_emit = Instant::now();
                        return Poll::Ready(Some(batch.into()));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

/// A stream that de-batches items from an inner stream.
///
/// Immediately emits buffered items one at a time, then polls the
/// inner stream for more batches.  Order of items is preserved.
///
pub struct DeBatchingStream<I, S>
where
    S: Stream<Item = Vec<I>>,
{
    stream: S,
    buffer: VecDeque<I>,
}

impl<I, S> DeBatchingStream<I, S>
where
    S: Stream<Item = Vec<I>>,
{
    pub fn new(stream: S, _batch_size: usize, _timeout: Duration) -> Self {
        Self {
            stream,
            buffer: VecDeque::default(),
        }
    }
}

impl<I, S> Stream for DeBatchingStream<I, S>
where
    S: Stream<Item = Vec<I>> + Unpin,
    I: Unpin,
{
    type Item = I;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // Drain buffered items first
            if let Some(item) = this.buffer.pop_front() {
                return Poll::Ready(Some(item));
            }

            // Buffer empty — poll the inner stream for the next batch
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(items)) => {
                    for item in items {
                        this.buffer.push_back(item);
                    }
                    // loop back to drain
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use crate::chunk_storage::Node;
    use crate::hash::hash as do_hash;

    use super::*;

    async fn batched_stream_roundtrip(nodes: &Vec<Node>) {
        let stream = tokio_stream::iter(nodes.clone());
        let sender = BatchingStream::new(stream, 32, Duration::new(4, 0));
        let mut receiver = DeBatchingStream::new(sender, 32, Duration::new(4, 0));

        let mut count = 0;
        while let Some(node) = receiver.next().await {
            assert_eq!(node, nodes[count]);
            count += 1;
        }

        assert_eq!(count, nodes.len());
    }

    #[tokio::test]
    async fn batched_stream_roundtrip_1() {
        let nodes = vec![Node::Stored {
            hash: do_hash(b""),
            data: Arc::default(),
        }];

        batched_stream_roundtrip(&nodes).await;
    }

    #[tokio::test]
    async fn batched_stream_roundtrip_some() {
        let nodes = vec![
            Node::Stored {
                hash: do_hash(b""),
                data: Arc::default(),
            },
            Node::Stored {
                hash: do_hash(b"somedata"),
                data: Arc::new(b"somedata".into()),
            },
            Node::Stored {
                hash: do_hash(b"1234"),
                data: Arc::new(b"1234".into()),
            },
        ];

        batched_stream_roundtrip(&nodes).await;
    }
}
