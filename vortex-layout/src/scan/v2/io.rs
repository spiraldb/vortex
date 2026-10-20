// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::request::Completion;
use vortex_io::request::IoBatch;
use vortex_io::request::IoIntent;
use vortex_io::request::IoOwnerId;
use vortex_io::request::IoResult;
use vortex_io::request::IoService;
use vortex_io::request::IoSource;
use vortex_io::request::IoTarget;
use vortex_utils::aliases::hash_map::HashMap;

use crate::scan::execution::SegmentLocation;
use crate::segments::SegmentFuture;
use crate::segments::SegmentId;
use crate::segments::SegmentSource;

/// Finds a file's segment by its byte range, to answer a range request with the segment.
#[derive(Clone)]
pub(super) enum SegmentRanges {
    /// Every location, indexed by segment id and in offset order, as writers lay segments out.
    /// Searched in place, so a scan builds nothing per segment.
    Sorted(Arc<[SegmentLocation]>),
    /// Segment ids keyed by `(offset, length)`, for locations in any other order.
    Keyed(Arc<HashMap<(u64, usize), SegmentId>>),
}

impl SegmentRanges {
    pub(super) fn new(locations: Arc<[SegmentLocation]>) -> Self {
        if locations.is_sorted_by_key(|location| location.offset) {
            return Self::Sorted(locations);
        }
        let ranges = locations.iter().enumerate().map(|(id, location)| {
            let id = SegmentId::from(u32::try_from(id).unwrap_or(u32::MAX));
            ((location.offset, location.length as usize), id)
        });
        Self::Keyed(Arc::new(ranges.collect()))
    }

    fn find(&self, offset: u64, len: usize) -> Option<SegmentId> {
        match self {
            Self::Sorted(locations) => {
                let first = locations.partition_point(|location| location.offset < offset);
                let index = locations[first..]
                    .iter()
                    .take_while(|location| location.offset == offset)
                    .position(|location| location.length as usize == len)?;
                u32::try_from(first + index).ok().map(SegmentId::from)
            }
            Self::Keyed(ranges) => ranges.get(&(offset, len)).copied(),
        }
    }
}

/// Serves splits from any [`SegmentSource`], by segment id.
pub(super) struct SegmentScanIo {
    segments: Arc<dyn SegmentSource>,
    ranges: SegmentRanges,
    shared: SharedReads,
}

impl SegmentScanIo {
    pub(super) fn new(segments: Arc<dyn SegmentSource>, ranges: SegmentRanges) -> Self {
        Self {
            segments,
            ranges,
            shared: SharedReads::default(),
        }
    }

    /// Expects one more session, the next planner's, to read each of `segments`, which the
    /// session of the planner handed out before it reads too. Each segment is read once for all
    /// the sessions expected to read it.
    pub(super) fn share_with_next(&self, segments: impl IntoIterator<Item = SegmentId>) {
        let mut shared = self.shared.lock();
        for segment in segments {
            shared
                .entry(segment)
                .or_insert(SharedRead {
                    readers: 1,
                    read: None,
                })
                .readers += 1;
        }
    }
}

impl IoService for SegmentScanIo {
    fn session(&self) -> Arc<dyn IoSource> {
        Arc::new(SegmentIoSource::new(
            Arc::clone(&self.segments),
            self.ranges.clone(),
            Arc::clone(&self.shared),
        ))
    }
}

/// Reads several sessions of one scan make of the same segment, by segment id.
type SharedReads = Arc<Mutex<HashMap<SegmentId, SharedRead>>>;

/// A segment read for several sessions: started by the first and dropped once each has taken it.
struct SharedRead {
    /// Sessions still to take the read.
    readers: usize,
    read: Option<Prefetched>,
}

/// Serves a split's range requests from a segment source, without blocking.
///
/// Each requested range names one segment. [`submit`](IoSource::submit) starts its read at once,
/// [`poll`](IoSource::poll) takes a finished one if there is any, and the split's future awaits
/// the next with [`poll_completion`](IoSource::poll_completion) when the driver has nothing else
/// to run. [`wait`](IoSource::wait) is never called.
///
/// An announcement requests the segment and holds the request, never polled, until a prefetch or
/// fetch consumes it, so a source that coalesces requests can fold it into a nearby read.
pub(super) struct SegmentIoSource {
    segments: Arc<dyn SegmentSource>,
    ranges: SegmentRanges,
    shared: SharedReads,
    reads: Mutex<FuturesUnordered<BoxFuture<'static, Completion>>>,
    ready: Mutex<VecDeque<Completion>>,
    /// Reads started by prefetches, consumed by the next fetch of their segment.
    prefetched: Mutex<HashMap<SegmentId, Prefetched>>,
    /// Announced segments, requested and never polled.
    announced: Mutex<HashMap<SegmentId, VecDeque<SegmentFuture>>>,
}

/// A read started ahead of the fetch that needs it.
type Prefetched = Shared<BoxFuture<'static, Result<BufferHandle, Arc<VortexError>>>>;

impl SegmentIoSource {
    fn new(segments: Arc<dyn SegmentSource>, ranges: SegmentRanges, shared: SharedReads) -> Self {
        Self {
            segments,
            ranges,
            shared,
            reads: Mutex::new(FuturesUnordered::new()),
            ready: Mutex::default(),
            prefetched: Mutex::default(),
            announced: Mutex::default(),
        }
    }

    /// Requests `segment`, from the read other sessions share when the scan shares it.
    fn request(&self, segment: SegmentId) -> SegmentFuture {
        let mut shared = self.shared.lock();
        let Some(entry) = shared.get_mut(&segment) else {
            return self.segments.request(segment);
        };
        let read = entry
            .read
            .get_or_insert_with(|| {
                self.segments
                    .request(segment)
                    .map_err(Arc::new)
                    .boxed()
                    .shared()
            })
            .clone();
        entry.readers = entry.readers.saturating_sub(1);
        if entry.readers == 0 {
            shared.remove(&segment);
        }
        read.map_err(move |err| vortex_err!("shared read of segment {segment} failed: {err}"))
            .boxed()
    }
}

impl IoSource for SegmentIoSource {
    fn submit(&self, owner: IoOwnerId, batch: IoBatch) -> VortexResult<()> {
        let reads = self.reads.lock();
        let mut ready = self.ready.lock();
        let mut prefetched = self.prefetched.lock();
        for request in batch {
            let IoTarget::Range { offset, len, .. } = request.target else {
                vortex_bail!(
                    "SegmentIoSource only serves byte ranges, not {:?}",
                    request.target
                );
            };
            let segment = self
                .ranges
                .find(offset, len)
                .ok_or_else(|| vortex_err!("No segment at bytes {offset}+{len}"))?;
            if request.intent == IoIntent::Announce {
                self.announced
                    .lock()
                    .entry(segment)
                    .or_default()
                    .push_back(self.request(segment));
                continue;
            }
            if request.intent == IoIntent::Prefetch {
                prefetched.entry(segment).or_insert_with(|| {
                    let read = self
                        .announced
                        .lock()
                        .get_mut(&segment)
                        .and_then(VecDeque::pop_front)
                        .unwrap_or_else(|| self.request(segment))
                        .map_err(Arc::new)
                        .boxed()
                        .shared();
                    // Polling registers the read as wanted, so the source starts it now.
                    drop(
                        read.clone()
                            .poll_unpin(&mut Context::from_waker(Waker::noop())),
                    );
                    read
                });
                continue;
            }
            let mut bytes = match prefetched.remove(&segment) {
                Some(read) => read
                    .map_err(move |err| {
                        vortex_err!("prefetched read of segment {segment} failed: {err}")
                    })
                    .boxed(),
                None => self
                    .announced
                    .lock()
                    .get_mut(&segment)
                    .and_then(VecDeque::pop_front)
                    .unwrap_or_else(|| self.request(segment)),
            };
            let id = request.request;
            // In-memory and cached segments need no allocated asynchronous task. A pending
            // read is polled again by FuturesUnordered before the driver can go to sleep.
            if let Poll::Ready(result) = bytes.poll_unpin(&mut Context::from_waker(Waker::noop())) {
                ready.push_back(Completion {
                    owner,
                    request: id,
                    result: result.map(IoResult::Bytes),
                });
                continue;
            }
            reads.push(
                async move {
                    Completion {
                        owner,
                        request: id,
                        result: bytes.await.map(IoResult::Bytes),
                    }
                }
                .boxed(),
            );
        }
        Ok(())
    }

    fn poll(&self) -> VortexResult<Option<Completion>> {
        if let Some(completion) = self.ready.lock().pop_front() {
            return Ok(Some(completion));
        }
        // The split's future polls again with its own waker before it waits, so a read that
        // finishes after this check is not missed.
        let mut cx = Context::from_waker(Waker::noop());
        match self.reads.lock().poll_next_unpin(&mut cx) {
            Poll::Ready(completion) => Ok(completion),
            Poll::Pending => Ok(None),
        }
    }

    fn poll_completion(&self, cx: &mut Context<'_>) -> Poll<VortexResult<Completion>> {
        if let Some(completion) = self.ready.lock().pop_front() {
            return Poll::Ready(Ok(completion));
        }
        match self.reads.lock().poll_next_unpin(cx) {
            Poll::Ready(Some(completion)) => Poll::Ready(Ok(completion)),
            Poll::Ready(None) => Poll::Ready(Err(vortex_err!(
                "The split's driver is waiting with no reads in flight"
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn wait(&self) -> VortexResult<Completion> {
        vortex_bail!("SegmentIoSource is awaited through poll_completion, not waited on")
    }

    fn release(&self, _owner: IoOwnerId) {}

    fn clear(&self) {
        self.reads.lock().clear();
        self.ready.lock().clear();
        self.prefetched.lock().clear();
        self.announced.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use futures::FutureExt;
    use futures::channel::oneshot;
    use futures::future;
    use parking_lot::Mutex;
    use vortex_array::buffer::BufferHandle;
    use vortex_buffer::Alignment;
    use vortex_buffer::ByteBuffer;
    use vortex_error::VortexResult;
    use vortex_error::vortex_err;
    use vortex_io::request::IoIntent;
    use vortex_io::request::IoOwnerId;
    use vortex_io::request::IoRequest;
    use vortex_io::request::IoRequestId;
    use vortex_io::request::IoService;
    use vortex_io::request::IoSource;
    use vortex_io::request::IoTarget;

    use super::SegmentIoSource;
    use super::SegmentRanges;
    use super::SegmentScanIo;
    use super::SharedReads;
    use crate::scan::execution::SegmentLocation;
    use crate::segments::SegmentFuture;
    use crate::segments::SegmentId;
    use crate::segments::SegmentSource;

    /// Serves four bytes per segment and counts the reads it is asked for.
    #[derive(Default)]
    struct CountingSegments(AtomicUsize);

    impl SegmentSource for CountingSegments {
        fn request(&self, id: SegmentId) -> SegmentFuture {
            self.0.fetch_add(1, Ordering::Relaxed);
            let bytes = ByteBuffer::from(vec![u8::try_from(*id).unwrap_or(u8::MAX); 4]);
            future::ready(Ok(BufferHandle::new_host(bytes))).boxed()
        }
    }

    fn location(offset: u64, length: u32) -> SegmentLocation {
        SegmentLocation {
            offset,
            length,
            alignment: Alignment::none(),
        }
    }

    /// Ranges are found by search when locations are in offset order, where a zero-length
    /// segment can share its offset with the next, and by key otherwise.
    #[test]
    fn segment_ranges_find_each_segment() {
        let sorted = SegmentRanges::new([location(0, 4), location(4, 0), location(4, 8)].into());
        assert!(matches!(sorted, SegmentRanges::Sorted(_)));
        let unsorted = SegmentRanges::new([location(4, 0), location(4, 8), location(0, 4)].into());
        assert!(matches!(unsorted, SegmentRanges::Keyed(_)));
        for (ranges, ids) in [(sorted, [0, 1, 2]), (unsorted, [2, 0, 1])] {
            let found = [(0, 4), (4, 0), (4, 8)].map(|(offset, len)| ranges.find(offset, len));
            assert_eq!(found, ids.map(|id| Some(SegmentId::from(id))));
            assert_eq!(ranges.find(4, 4), None);
        }
    }

    fn request(intent: IoIntent, id: u32) -> IoRequest {
        IoRequest {
            intent,
            request: IoRequestId(id),
            target: IoTarget::range(0, 4),
        }
    }

    struct MixedSegments(Mutex<Option<oneshot::Receiver<ByteBuffer>>>);

    impl SegmentSource for MixedSegments {
        fn request(&self, id: SegmentId) -> SegmentFuture {
            if *id == 0 {
                return future::ready(Ok(BufferHandle::new_host(ByteBuffer::from(vec![0; 4]))))
                    .boxed();
            }
            let receiver = self.0.lock().take().expect("one pending read");
            async move {
                receiver
                    .await
                    .map(BufferHandle::new_host)
                    .map_err(|err| vortex_err!("{err}"))
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn pending_read_wakes_after_an_immediate_completion() -> VortexResult<()> {
        let (sender, receiver) = oneshot::channel();
        let segments = Arc::new(MixedSegments(Mutex::new(Some(receiver))));
        let locations = [0, 4].map(|offset| SegmentLocation {
            offset,
            length: 4,
            alignment: Alignment::none(),
        });
        let io = SegmentIoSource::new(
            segments,
            SegmentRanges::new(locations.into()),
            SharedReads::default(),
        );
        let mut pending = request(IoIntent::Fetch, 1);
        pending.target = IoTarget::range(4, 4);
        io.submit(IoOwnerId(0), vec![pending, request(IoIntent::Fetch, 0)])?;
        let immediate = io.poll()?.expect("the in-memory read is already ready");
        assert_eq!(immediate.request, IoRequestId(0));
        assert!(immediate.result.is_ok());
        assert!(io.poll()?.is_none());

        let delivery = tokio::spawn(async move {
            tokio::task::yield_now().await;
            sender
                .send(ByteBuffer::from(vec![1; 4]))
                .expect("read is live");
        });
        let completed = std::future::poll_fn(|cx| io.poll_completion(cx)).await?;
        assert_eq!(completed.request, IoRequestId(1));
        assert!(completed.result.is_ok());
        delivery.await.expect("delivery task succeeds");
        assert!(io.poll()?.is_none());
        Ok(())
    }

    /// Sessions of a scan that share a segment read it once, and a later session reads it again
    /// once every expected reader has taken it.
    #[tokio::test]
    async fn sessions_share_a_segment_read() -> VortexResult<()> {
        let segments = Arc::new(CountingSegments::default());
        let io = SegmentScanIo::new(
            Arc::clone(&segments) as _,
            SegmentRanges::new([location(0, 4)].into()),
        );
        io.share_with_next([SegmentId::from(0)]);
        for reads in [1, 1, 2] {
            let session = io.session();
            session.submit(IoOwnerId(0), vec![request(IoIntent::Fetch, 0)])?;
            let completion = std::future::poll_fn(|cx| session.poll_completion(cx)).await?;
            assert!(completion.result.is_ok());
            assert_eq!(segments.0.load(Ordering::Relaxed), reads);
        }
        Ok(())
    }

    /// A fetch of a prefetched segment waits on the prefetch's read rather than reading again,
    /// and only the fetch is delivered.
    #[tokio::test]
    async fn fetch_reuses_a_prefetched_read() -> VortexResult<()> {
        let segments = Arc::new(CountingSegments::default());
        let location = SegmentLocation {
            offset: 0,
            length: 4,
            alignment: Alignment::none(),
        };
        let io = SegmentIoSource::new(
            Arc::clone(&segments) as _,
            SegmentRanges::new([location].into()),
            SharedReads::default(),
        );

        io.submit(IoOwnerId(0), vec![request(IoIntent::Prefetch, 0)])?;
        assert_eq!(segments.0.load(Ordering::Relaxed), 1);
        io.submit(IoOwnerId(0), vec![request(IoIntent::Fetch, 1)])?;
        assert_eq!(segments.0.load(Ordering::Relaxed), 1);

        let completion = std::future::poll_fn(|cx| io.poll_completion(cx)).await?;
        assert_eq!(completion.request, IoRequestId(1));
        assert!(completion.result.is_ok());
        assert!(io.poll()?.is_none());

        io.submit(IoOwnerId(0), vec![request(IoIntent::Fetch, 2)])?;
        let completion = std::future::poll_fn(|cx| io.poll_completion(cx)).await?;
        assert_eq!(completion.request, IoRequestId(2));
        assert!(completion.result.is_ok());
        assert_eq!(segments.0.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[tokio::test]
    async fn announcements_keep_independent_consumers() -> VortexResult<()> {
        let segments = Arc::new(CountingSegments::default());
        let location = SegmentLocation {
            offset: 0,
            length: 4,
            alignment: Alignment::none(),
        };
        let io = SegmentIoSource::new(
            Arc::clone(&segments) as _,
            SegmentRanges::new([location].into()),
            SharedReads::default(),
        );
        io.submit(
            IoOwnerId(0),
            vec![
                request(IoIntent::Announce, 0),
                request(IoIntent::Announce, 1),
            ],
        )?;
        assert_eq!(segments.0.load(Ordering::Relaxed), 2);
        for id in 2..5 {
            io.submit(IoOwnerId(0), vec![request(IoIntent::Fetch, id)])?;
            assert!(
                std::future::poll_fn(|cx| io.poll_completion(cx))
                    .await?
                    .result
                    .is_ok()
            );
            assert_eq!(
                segments.0.load(Ordering::Relaxed),
                if id < 4 { 2 } else { 3 }
            );
        }
        Ok(())
    }
}
