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
use smallvec::SmallVec;
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

use crate::scan::planning::SegmentLocation;
use crate::segments::SegmentFuture;
use crate::segments::SegmentId;
use crate::segments::SegmentSource;

/// Byte ranges of a file's segments, keyed by `(offset, length)`, back to their segment ids.
pub(super) type SegmentRanges = Arc<RangeLookup>;

pub(super) enum RangeLookup {
    Ordered(Arc<[SegmentLocation]>),
    Unordered(HashMap<(u64, usize), SegmentId>),
}

impl RangeLookup {
    pub(super) fn get(&self, key: &(u64, usize)) -> Option<SegmentId> {
        match self {
            Self::Ordered(locations) => {
                let index = locations
                    .partition_point(|location| (location.offset, location.length as usize) <= *key)
                    .checked_sub(1)?;
                let location = &locations[index];
                ((location.offset, location.length as usize) == *key)
                    .then(|| SegmentId::from(u32::try_from(index).unwrap_or(u32::MAX)))
            }
            Self::Unordered(ranges) => ranges.get(key).copied(),
        }
    }
}

pub(super) fn segment_ranges(locations: &Arc<[SegmentLocation]>) -> SegmentRanges {
    // File writers normally emit segments in byte order. Reuse that index instead of allocating
    // a table for the entire file when a scan may read only a handful of its segments.
    if locations.is_sorted_by_key(|location| (location.offset, location.length)) {
        return Arc::new(RangeLookup::Ordered(Arc::clone(locations)));
    }
    let ranges = locations.iter().enumerate().map(|(id, location)| {
        let id = SegmentId::from(u32::try_from(id).unwrap_or(u32::MAX));
        ((location.offset, location.length as usize), id)
    });
    Arc::new(RangeLookup::Unordered(ranges.collect()))
}

/// Registers known sparse-scan segments before any fetch starts the file's read driver.
/// Neighboring tasks can then share coalesced reads instead of reading overlapping byte ranges.
pub(super) fn register_segments(
    source: Arc<dyn SegmentSource>,
    mut segments: Vec<SegmentId>,
) -> Arc<dyn SegmentSource> {
    if segments.is_empty() {
        return source;
    }
    segments.sort_unstable();
    segments.dedup();
    let registered = segments
        .into_iter()
        .map(|id| (id, source.request(id)))
        .collect();
    Arc::new(RegisteredSegments {
        source,
        registered: Mutex::new(registered),
    })
}

struct RegisteredSegments {
    source: Arc<dyn SegmentSource>,
    registered: Mutex<HashMap<SegmentId, SegmentFuture>>,
}

impl SegmentSource for RegisteredSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        self.registered
            .lock()
            .remove(&id)
            .unwrap_or_else(|| self.source.request(id))
    }
}

/// Serves splits from any [`SegmentSource`], by segment id.
pub(super) struct SegmentScanIo {
    segments: Arc<dyn SegmentSource>,
    ranges: SegmentRanges,
}

impl SegmentScanIo {
    pub(super) fn new(segments: Arc<dyn SegmentSource>, ranges: SegmentRanges) -> Self {
        Self { segments, ranges }
    }
}

impl IoService for SegmentScanIo {
    fn session(&self) -> Arc<dyn IoSource> {
        Arc::new(SegmentIoSource::new(
            Arc::clone(&self.segments),
            Arc::clone(&self.ranges),
        ))
    }
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
    state: Mutex<SegmentIoState>,
}

#[derive(Default)]
struct SegmentIoState {
    reads: FuturesUnordered<BoxFuture<'static, Completion>>,
    ready: VecDeque<Completion>,
    /// Reads started by prefetches, consumed by the next fetch of their segment.
    prefetched: HashMap<SegmentId, Prefetched>,
    /// Announced segments, requested and never polled.
    announced: HashMap<SegmentId, VecDeque<SegmentFuture>>,
}

/// A read started ahead of the fetch that needs it.
type Prefetched = Shared<BoxFuture<'static, Result<BufferHandle, Arc<VortexError>>>>;

impl SegmentIoSource {
    pub(super) fn new(segments: Arc<dyn SegmentSource>, ranges: SegmentRanges) -> Self {
        Self {
            segments,
            ranges,
            state: Mutex::default(),
        }
    }
}

impl IoSource for SegmentIoSource {
    fn submit(&self, owner: IoOwnerId, batch: IoBatch) -> VortexResult<()> {
        let mut state = self.state.lock();
        let SegmentIoState {
            reads,
            ready,
            prefetched,
            announced,
        } = &mut *state;
        let mut fetched = SmallVec::<[_; 4]>::with_capacity(batch.len());
        for request in batch {
            let IoTarget::Range { offset, len, .. } = request.target else {
                vortex_bail!(
                    "SegmentIoSource only serves byte ranges, not {:?}",
                    request.target
                );
            };
            let segment = self
                .ranges
                .get(&(offset, len))
                .ok_or_else(|| vortex_err!("No segment at bytes {offset}+{len}"))?;
            if request.intent == IoIntent::Announce {
                announced
                    .entry(segment)
                    .or_default()
                    .push_back(self.segments.request(segment));
                continue;
            }
            if request.intent == IoIntent::Prefetch {
                prefetched.entry(segment).or_insert_with(|| {
                    let read = announced
                        .get_mut(&segment)
                        .and_then(VecDeque::pop_front)
                        .unwrap_or_else(|| self.segments.request(segment))
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
            let bytes = match prefetched.remove(&segment) {
                Some(read) => read
                    .map_err(move |err| {
                        vortex_err!("prefetched read of segment {segment} failed: {err}")
                    })
                    .boxed(),
                None => announced
                    .get_mut(&segment)
                    .and_then(VecDeque::pop_front)
                    .unwrap_or_else(|| self.segments.request(segment)),
            };
            fetched.push((request.request, bytes));
        }
        // Register the whole batch before polling its fetches. A file source can coalesce
        // registered neighbors when the first fetch starts the read driver.
        for (id, mut bytes) in fetched {
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
        let mut state = self.state.lock();
        if let Some(completion) = state.ready.pop_front() {
            return Ok(Some(completion));
        }
        // The split's future polls again with its own waker before it waits, so a read that
        // finishes after this check is not missed.
        let mut cx = Context::from_waker(Waker::noop());
        match state.reads.poll_next_unpin(&mut cx) {
            Poll::Ready(completion) => Ok(completion),
            Poll::Pending => Ok(None),
        }
    }

    fn poll_completion(&self, cx: &mut Context<'_>) -> Poll<VortexResult<Completion>> {
        let mut state = self.state.lock();
        if let Some(completion) = state.ready.pop_front() {
            return Poll::Ready(Ok(completion));
        }
        match state.reads.poll_next_unpin(cx) {
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
        let mut state = self.state.lock();
        state.reads.clear();
        state.ready.clear();
        state.prefetched.clear();
        state.announced.clear();
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
    use rstest::rstest;
    use vortex_array::buffer::BufferHandle;
    use vortex_buffer::Alignment;
    use vortex_buffer::ByteBuffer;
    use vortex_error::VortexResult;
    use vortex_error::vortex_err;
    use vortex_io::request::IoIntent;
    use vortex_io::request::IoOwnerId;
    use vortex_io::request::IoRequest;
    use vortex_io::request::IoRequestId;
    use vortex_io::request::IoSource;
    use vortex_io::request::IoTarget;

    use super::SegmentIoSource;
    use super::register_segments;
    use super::segment_ranges;
    use crate::scan::planning::SegmentLocation;
    use crate::segments::SegmentFuture;
    use crate::segments::SegmentId;
    use crate::segments::SegmentSource;

    #[rstest]
    #[case::ordered(vec![0, 4, 8], [Some(0), Some(1), Some(2), None, None])]
    #[case::unordered(vec![8, 0, 4], [Some(1), Some(2), Some(0), None, None])]
    #[case::duplicates(vec![0, 4, 4], [Some(0), Some(2), None, None, None])]
    #[case::empty(vec![], [None; 5])]
    fn byte_ranges_resolve_to_segment_ids(
        #[case] offsets: Vec<u64>,
        #[case] expected: [Option<u32>; 5],
    ) {
        let locations = offsets
            .into_iter()
            .map(|offset| SegmentLocation {
                offset,
                length: 4,
                alignment: Alignment::none(),
            })
            .collect();
        let ranges = segment_ranges(&locations);
        for (key, expected) in [(0, 4), (4, 4), (8, 4), (4, 3), (12, 4)]
            .into_iter()
            .zip(expected)
        {
            assert_eq!(ranges.get(&key).map(|id| *id), expected);
        }
    }

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

    #[tokio::test]
    async fn registered_segments_are_consumed_once() -> VortexResult<()> {
        let source = Arc::new(CountingSegments::default());
        let segments = register_segments(
            Arc::clone(&source) as _,
            vec![SegmentId::from(0), SegmentId::from(1), SegmentId::from(0)],
        );
        assert_eq!(source.0.load(Ordering::Relaxed), 2);
        for (id, calls) in [(1, 2), (0, 2), (0, 3), (2, 4)] {
            let bytes = segments.request(SegmentId::from(id)).await?;
            assert_eq!(
                bytes.try_into_host_sync()?.as_ref(),
                &[u8::try_from(id)?; 4]
            );
            assert_eq!(source.0.load(Ordering::Relaxed), calls);
        }
        Ok(())
    }

    fn request(intent: IoIntent, id: u32) -> IoRequest {
        IoRequest {
            intent,
            request: IoRequestId(id),
            target: IoTarget::range(0, 4),
        }
    }

    struct MixedSegments(Mutex<Option<oneshot::Receiver<ByteBuffer>>>);

    #[derive(Default)]
    struct BatchSegments {
        registered: Arc<AtomicUsize>,
        observed: Arc<Mutex<Vec<usize>>>,
    }

    impl SegmentSource for BatchSegments {
        fn request(&self, _id: SegmentId) -> SegmentFuture {
            self.registered.fetch_add(1, Ordering::Relaxed);
            let registered = Arc::clone(&self.registered);
            let observed = Arc::clone(&self.observed);
            async move {
                observed.lock().push(registered.load(Ordering::Relaxed));
                Ok(BufferHandle::new_host(ByteBuffer::from(vec![0; 4])))
            }
            .boxed()
        }
    }

    #[test]
    fn registers_fetch_batch_before_polling() -> VortexResult<()> {
        let segments = Arc::new(BatchSegments::default());
        let locations = [0, 4, 8].map(|offset| SegmentLocation {
            offset,
            length: 4,
            alignment: Alignment::none(),
        });
        let io = SegmentIoSource::new(
            Arc::clone(&segments) as _,
            segment_ranges(&locations.into()),
        );
        let batch = (0..3)
            .map(|id| IoRequest {
                intent: IoIntent::Fetch,
                request: IoRequestId(id),
                target: IoTarget::range(u64::from(id) * 4, 4),
            })
            .collect();
        io.submit(IoOwnerId(0), batch)?;
        assert_eq!(*segments.observed.lock(), vec![3, 3, 3]);
        for id in 0..3 {
            assert_eq!(io.poll()?.expect("ready fetch").request, IoRequestId(id));
        }
        assert!(io.poll()?.is_none());
        Ok(())
    }

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
        let io = SegmentIoSource::new(segments, segment_ranges(&locations.into()));
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
            segment_ranges(&Arc::from([location])),
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
            segment_ranges(&Arc::from([location])),
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
