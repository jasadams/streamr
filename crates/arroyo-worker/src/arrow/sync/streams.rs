use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::ready;
use futures::{Future, StreamExt};
use tokio_stream::Stream;

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};

/// Apply a per-batch limit and account output against the worker execution pool.
/// No task or queue is created: the producer advances only when its caller polls.
/// The reservation covers the yielded batch until the next poll or stream drop,
/// so callers must consume/drop each batch before requesting another. Retaining
/// or cloning batches beyond that boundary requires separate caller accounting.
///
/// The producer has already allocated a batch before this wrapper can inspect it.
/// Cooperative DataFusion operators must use this same pool for their allocations;
/// this boundary alone does not bound arbitrary scalar/UDF allocations or RSS.
pub(crate) fn bounded_output_stream(
    stream: SendableRecordBatchStream,
    memory_pool: Arc<dyn MemoryPool>,
    max_batch_bytes: usize,
) -> SendableRecordBatchStream {
    Box::pin(BoundedOutputStream {
        schema: stream.schema(),
        inner: Some(stream),
        reservation: MemoryConsumer::new("Streamr execution output")
            .with_can_spill(false)
            .register(&memory_pool),
        max_batch_bytes,
    })
}

struct BoundedOutputStream {
    schema: SchemaRef,
    inner: Option<SendableRecordBatchStream>,
    reservation: MemoryReservation,
    max_batch_bytes: usize,
}

impl BoundedOutputStream {
    fn stop(&mut self) {
        self.inner.take();
        self.reservation.free();
    }
}

impl Stream for BoundedOutputStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // A new poll means the caller has consumed the previous output batch.
        this.reservation.free();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        if this.max_batch_bytes == 0 {
            this.stop();
            return Poll::Ready(Some(Err(DataFusionError::ResourcesExhausted(
                "execution output max_batch_bytes must be greater than zero".into(),
            ))));
        }
        match ready!(inner.as_mut().poll_next(cx)) {
            Some(Ok(batch)) => {
                let bytes = batch.get_array_memory_size();
                if bytes > this.max_batch_bytes {
                    this.stop();
                    return Poll::Ready(Some(Err(DataFusionError::ResourcesExhausted(format!(
                        "execution output batch requires {bytes} bytes, exceeding max_batch_bytes {}",
                        this.max_batch_bytes
                    )))));
                }
                if let Err(error) = this.reservation.try_resize(bytes) {
                    this.stop();
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(batch)))
            }
            Some(Err(error)) => {
                this.stop();
                Poll::Ready(Some(Err(error)))
            }
            None => {
                this.stop();
                Poll::Ready(None)
            }
        }
    }
}

impl RecordBatchStream for BoundedOutputStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub struct CloneableStreamFuture<St: Stream + Unpin> {
    stream: Arc<Mutex<Option<St>>>,
}

impl<St: Stream + Unpin> Clone for CloneableStreamFuture<St> {
    fn clone(&self) -> Self {
        Self {
            stream: self.stream.clone(),
        }
    }
}

impl<St: Stream + Unpin> Future for CloneableStreamFuture<St> {
    type Output = Option<(St::Item, Self)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut guard = self
            .stream
            .try_lock()
            .expect("mutex should stay in a single execution sequence");

        let stream_option = guard.as_mut();
        if let Some(stream) = stream_option {
            let item = ready!(stream.poll_next_unpin(cx));
            match item {
                Some(batch) => {
                    let next_future = self.clone();
                    Poll::Ready(Some((batch, next_future)))
                }
                None => {
                    *guard = None;
                    Poll::Ready(None)
                }
            }
        } else {
            // Stream is already finished
            Poll::Ready(None)
        }
    }
}

pub struct KeyedCloneableStreamFuture<K, St: Stream + Unpin> {
    key: K,
    // Wrap CloneableStreamFuture inside KeyedCloneableStreamFuture.
    future: CloneableStreamFuture<St>,
}

impl<K: Copy, St: Stream + Unpin> KeyedCloneableStreamFuture<K, St> {
    pub fn new(key: K, stream: St) -> Self {
        Self {
            key,
            future: CloneableStreamFuture {
                stream: Arc::new(Mutex::new(Some(stream))),
            },
        }
    }
}

impl<K: Copy, St: Stream + Unpin> Clone for KeyedCloneableStreamFuture<K, St> {
    fn clone(&self) -> Self {
        Self {
            key: self.key,
            future: self.future.clone(),
        }
    }
}

impl<K: Copy, St: Stream + Unpin> Future for KeyedCloneableStreamFuture<K, St> {
    type Output = (K, Option<(St::Item, Self)>);

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let key = self.key;
        let future = unsafe { self.map_unchecked_mut(|s| &mut s.future) };

        // Now you can safely call poll on the pinned future.
        match ready!(future.poll(cx)) {
            Some((item, next_future)) => {
                let next_keyed_future = Self {
                    key,
                    future: next_future,
                };
                Poll::Ready((key, Some((item, next_keyed_future))))
            }
            None => Poll::Ready((key, None)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::UInt64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::FairSpillPool;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct ObservedStream {
        batches: VecDeque<DFResult<RecordBatch>>,
        polls: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        pending: bool,
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            false,
        )]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(UInt64Array::from(vec![1; 16]))]).unwrap()
    }

    impl Stream for ObservedStream {
        type Item = DFResult<RecordBatch>;
        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            this.polls.fetch_add(1, Ordering::SeqCst);
            if this.pending {
                Poll::Pending
            } else {
                Poll::Ready(this.batches.pop_front())
            }
        }
    }

    impl RecordBatchStream for ObservedStream {
        fn schema(&self) -> SchemaRef {
            schema()
        }
    }

    impl Drop for ObservedStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    fn observed(
        batches: Vec<DFResult<RecordBatch>>,
        pending: bool,
    ) -> (SendableRecordBatchStream, Arc<AtomicUsize>, Arc<AtomicBool>) {
        let polls = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicBool::new(false));
        (
            Box::pin(ObservedStream {
                batches: batches.into(),
                polls: polls.clone(),
                dropped: dropped.clone(),
                pending,
            }),
            polls,
            dropped,
        )
    }

    #[tokio::test]
    async fn slow_consumer_does_not_prefetch_and_releases_output_at_completion() {
        let bytes = batch().get_array_memory_size();
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(bytes));
        let (inner, polls, dropped) = observed(vec![Ok(batch()), Ok(batch())], false);
        let mut stream = bounded_output_stream(inner, pool.clone(), bytes);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(pool.reserved(), bytes);
        tokio::task::yield_now().await;
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(first);
        drop(stream.next().await.unwrap().unwrap());
        assert_eq!(pool.reserved(), bytes);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(stream.next().await.is_none());
        assert_eq!(pool.reserved(), 0);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn concurrent_outputs_share_pool_and_fail_before_unaccounted_delivery() {
        let bytes = batch().get_array_memory_size();
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(bytes));
        let (inner, _, _) = observed(vec![Ok(batch())], false);
        let mut first = bounded_output_stream(inner, pool.clone(), bytes);
        let output = first.next().await.unwrap().unwrap();
        let (inner, _, dropped) = observed(vec![Ok(batch())], false);
        let mut second = bounded_output_stream(inner, pool.clone(), bytes);
        assert!(matches!(
            second.next().await,
            Some(Err(DataFusionError::ResourcesExhausted(_)))
        ));
        assert!(dropped.load(Ordering::SeqCst));
        assert!(second.next().await.is_none());
        assert_eq!(pool.reserved(), bytes);
        drop(output);
        drop(first);
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn oversized_output_fails_without_forwarding_or_continuing_execution() {
        let bytes = batch().get_array_memory_size();
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(bytes));
        let (inner, polls, dropped) = observed(vec![Ok(batch()), Ok(batch())], false);
        let mut stream = bounded_output_stream(inner, pool.clone(), bytes - 1);
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(matches!(error, DataFusionError::ResourcesExhausted(_)));
        assert!(error.to_string().contains("exceeding max_batch_bytes"));
        assert!(stream.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn cancellation_drops_pending_execution_and_held_output_reservation() {
        let bytes = batch().get_array_memory_size();
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(bytes));
        let (inner, _, dropped) = observed(vec![Ok(batch())], false);
        let mut stream = bounded_output_stream(inner, pool.clone(), bytes);
        drop(stream.next().await.unwrap().unwrap());
        assert_eq!(pool.reserved(), bytes);
        drop(stream);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(pool.reserved(), 0);

        let (inner, _, dropped) = observed(vec![], true);
        let mut stream = bounded_output_stream(inner, pool.clone(), bytes);
        assert!(futures::poll!(stream.next()).is_pending());
        drop(stream);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(pool.reserved(), 0);
    }

    #[tokio::test]
    async fn upstream_errors_preserve_type_and_message_and_release_execution() {
        let pool: Arc<dyn MemoryPool> = Arc::new(FairSpillPool::new(1024));
        let (inner, _, dropped) = observed(
            vec![Err(DataFusionError::Execution("original error".into()))],
            false,
        );
        let mut stream = bounded_output_stream(inner, pool.clone(), 1024);
        assert!(matches!(
            stream.next().await,
            Some(Err(DataFusionError::Execution(message))) if message == "original error"
        ));
        assert!(stream.next().await.is_none());
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(pool.reserved(), 0);
    }
}
