// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;

use futures::FutureExt;
use futures::TryFutureExt;
use futures::future::BoxFuture;
use futures::future::WeakShared;
use vortex_array::buffer::BufferHandle;
use vortex_buffer::Alignment;
use vortex_error::SharedVortexResult;
use vortex_error::VortexError;
use vortex_error::VortexExpect;
use vortex_utils::aliases::dash_map::DashMap;
use vortex_utils::aliases::dash_map::Entry;

use crate::segments::SegmentFuture;
use crate::segments::SegmentId;
use crate::segments::SegmentSource;
use crate::segments::source::slice_segment;

/// A [`SegmentSource`] that allows multiple requesters to await the same underlying segment
/// request.
///
/// Range requests are shared with identical in-flight range requests, and are served by slicing
/// the whole segment when it is already in flight.
pub struct SharedSegmentSource<S> {
    inner: S,
    in_flight: DashMap<RequestKey, WeakShared<SharedSegmentFuture>>,
}

/// A whole segment (`None`) or an aligned byte range of it.
type RequestKey = (SegmentId, Option<(Range<usize>, Alignment)>);

type SharedSegmentFuture = BoxFuture<'static, SharedVortexResult<BufferHandle>>;

impl<S: SegmentSource> SharedSegmentSource<S> {
    /// Create a new `SharedSegmentSource` wrapping the provided inner source.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            in_flight: DashMap::default(),
        }
    }

    fn shared_request(
        &self,
        key: RequestKey,
        request: impl FnOnce(&S) -> SegmentFuture,
    ) -> SegmentFuture {
        let mut request = Some(request);
        loop {
            match self.in_flight.entry(key.clone()) {
                Entry::Occupied(e) => {
                    if let Some(shared_future) = e.get().upgrade() {
                        return shared_future.map_err(VortexError::from).boxed();
                    } else {
                        // The future has been dropped, remove the entry and try again.
                        e.remove();
                    }
                }
                Entry::Vacant(e) => {
                    let request = request
                        .take()
                        .vortex_expect("a vacant entry is only visited once");
                    let future = request(&self.inner).map_err(Arc::new).boxed().shared();
                    e.insert(
                        future
                            .downgrade()
                            .vortex_expect("just created, cannot be polled to completion"),
                    );
                    return future.map_err(VortexError::from).boxed();
                }
            }
        }
    }
}

impl<S: SegmentSource> SegmentSource for SharedSegmentSource<S> {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.shared_request((id, None), |inner| inner.request(id))
    }

    fn request_range(
        &self,
        id: SegmentId,
        range: Range<usize>,
        alignment: Alignment,
    ) -> SegmentFuture {
        if let Some(whole) = self
            .in_flight
            .get(&(id, None))
            .and_then(|entry| entry.upgrade())
        {
            return whole
                .map(move |segment| slice_segment(segment?, id, range, alignment))
                .boxed();
        }
        self.shared_request((id, Some((range.clone(), alignment))), |inner| {
            inner.request_range(id, range, alignment)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use vortex_buffer::ByteBuffer;

    use super::*;
    use crate::segments::SegmentSink;
    use crate::segments::TestSegments;
    use crate::sequence::SequenceId;

    // Custom source that tracks how many times a segment is requested
    #[derive(Default, Clone)]
    struct CountingSegmentSource {
        segments: TestSegments,
        request_count: Arc<AtomicUsize>,
    }

    impl SegmentSource for CountingSegmentSource {
        fn request(&self, id: SegmentId) -> SegmentFuture {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            self.segments.request(id)
        }
    }

    #[tokio::test]
    async fn test_shared_source_deduplicates_concurrent_requests() {
        let source = CountingSegmentSource::default();

        // Add a segment to the test source
        let data = ByteBuffer::from(vec![1, 2, 3, 4]);
        let seq_id = SequenceId::root().downgrade();
        source
            .segments
            .write(seq_id, vec![data.clone()])
            .await
            .unwrap();

        let shared_source = SharedSegmentSource::new(source.clone());

        // Request the same segment twice concurrently
        let id = SegmentId::from(0);
        let future1 = shared_source.request(id);
        let future2 = shared_source.request(id);

        // Both futures should resolve to the same data
        let (result1, result2) = futures::join!(future1, future2);
        assert_eq!(result1.unwrap().unwrap_host(), data);
        assert_eq!(result2.unwrap().unwrap_host(), data);

        // The inner source should have been called only once
        assert_eq!(source.request_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_shared_source_handles_dropped_futures() {
        let source = CountingSegmentSource::default();

        // Add a segment
        let data = ByteBuffer::from(vec![5, 6, 7, 8]);
        let seq_id = SequenceId::root().downgrade();
        source
            .segments
            .write(seq_id, vec![data.clone()])
            .await
            .unwrap();

        let shared_source = SharedSegmentSource::new(source.clone());
        let id = SegmentId::from(0);

        // Create and immediately drop a future
        {
            let _future = shared_source.request(id);
            // Future is dropped here
        }

        // A new request should still work correctly
        let result = shared_source.request(id).await;
        assert_eq!(result.unwrap().unwrap_host(), data);

        // Should have made 2 requests since the first was dropped before completion
        assert_eq!(source.request_count.load(Ordering::Relaxed), 2);
    }
}
