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
use vortex_io::request::IoRequest;
use vortex_io::request::IoResult;
use vortex_io::request::IoService;
use vortex_io::request::IoSource;
use vortex_io::request::IoTarget;
use vortex_utils::aliases::hash_map::HashMap;

use crate::scan::execution::SegmentLocation;
use crate::segments::SegmentFuture;
use crate::segments::SegmentId;
use crate::segments::SegmentSource;

/// Byte ranges of a file's segments, keyed by `(offset, length)`, back to their segment ids.
pub(crate) type SegmentRanges = Arc<HashMap<(u64, usize), SegmentId>>;

pub(super) fn segment_ranges(locations: &[SegmentLocation]) -> SegmentRanges {
    let ranges = locations.iter().enumerate().map(|(id, location)| {
        let id = SegmentId::from(u32::try_from(id).unwrap_or(u32::MAX));
        ((location.offset, location.length as usize), id)
    });
    Arc::new(ranges.collect())
}

/// Serves splits from any [`SegmentSource`], by segment id.
///
/// Reads a session speculates or prefetches are kept with the file, shared by every session of
/// every scan of it: a chunk fetches what the planner before it read ahead on its behalf, and a
/// second scan of the file fetches what the first started.
pub(crate) struct SegmentScanIo {
    segments: Arc<dyn SegmentSource>,
    ranges: SegmentRanges,
    ahead: Ahead,
}

impl SegmentScanIo {
    /// A service over `segments`, sharing `ahead`, the reads speculated for the file, with
    /// every other scan of it.
    pub(crate) fn new(
        segments: Arc<dyn SegmentSource>,
        ranges: SegmentRanges,
        ahead: Ahead,
    ) -> Self {
        Self {
            segments,
            ranges,
            ahead,
        }
    }
}

impl IoService for SegmentScanIo {
    fn session(&self) -> Arc<dyn IoSource> {
        Arc::new(SegmentIoSource::new(
            Arc::clone(&self.segments),
            Arc::clone(&self.ranges),
            Arc::clone(&self.ahead),
        ))
    }
}

/// Reads a session speculated or prefetched: started reads, shared by every session that
/// fetches the segment for as long as the service lives, since a planner reads ahead for chunks
/// that run on sessions of their own and a chunk straddling two partitions is fetched by both;
/// and requests announced for coalescing and never polled, which the first fetch turns into a
/// shared started read.
#[derive(Default)]
pub(crate) struct Speculated {
    started: HashMap<SegmentId, Prefetched>,
    announced: HashMap<SegmentId, SegmentFuture>,
}

pub(crate) type Ahead = Arc<Mutex<Speculated>>;

/// Serves a split's range requests from a segment source, without blocking.
///
/// Each requested range names one segment. [`submit`](IoSource::submit) starts its read at once,
/// [`poll`](IoSource::poll) takes a finished one if there is any, and the split's future awaits
/// the next with [`poll_completion`](IoSource::poll_completion) when the driver has nothing else
/// to run. [`wait`](IoSource::wait) is never called.
///
/// An announcement requests the segment for the file and holds the request, never polled, until
/// a prefetch or fetch of any session consumes it, so a source that coalesces requests can fold
/// it into a nearby read.
pub(super) struct SegmentIoSource {
    segments: Arc<dyn SegmentSource>,
    ranges: SegmentRanges,
    reads: Mutex<FuturesUnordered<BoxFuture<'static, Completion>>>,
    ready: Mutex<VecDeque<Completion>>,
    /// Every read the session has fetched, by segment, shared by later fetches of the segment
    /// until the session is cleared.
    prefetched: Mutex<HashMap<SegmentId, Prefetched>>,
    /// Reads speculated by any session of the service, taken by the first fetch of each.
    ahead: Ahead,
}

/// A read started ahead of the fetch that needs it.
type Prefetched = Shared<BoxFuture<'static, Result<BufferHandle, Arc<VortexError>>>>;

impl SegmentIoSource {
    fn new(segments: Arc<dyn SegmentSource>, ranges: SegmentRanges, ahead: Ahead) -> Self {
        Self {
            segments,
            ranges,
            reads: Mutex::new(FuturesUnordered::new()),
            ready: Mutex::default(),
            prefetched: Mutex::default(),
            ahead,
        }
    }

    fn segment(&self, request: &IoRequest) -> VortexResult<SegmentId> {
        let IoTarget::Range { offset, len, .. } = request.target else {
            vortex_bail!(
                "SegmentIoSource only serves byte ranges, not {:?}",
                request.target
            );
        };
        self.ranges
            .get(&(offset, len))
            .copied()
            .ok_or_else(|| vortex_err!("No segment at bytes {offset}+{len}"))
    }

    /// The read of `segment`: one started for it, else the request announced for it, which
    /// becomes the started read every session shares, else a new request.
    fn read(&self, segment: SegmentId) -> Prefetched {
        let mut ahead = self.ahead.lock();
        if let Some(read) = ahead.started.get(&segment) {
            return read.clone();
        }
        if let Some(announced) = ahead.announced.remove(&segment) {
            return announced.map_err(Arc::new).boxed().shared();
        }
        tracing::trace!(segment = *segment, "v2 io new request on read");
        self.segments
            .request(segment)
            .map_err(Arc::new)
            .boxed()
            .shared()
    }

    /// Starts `read` now: polling registers it as wanted by the source.
    fn start(read: &Prefetched) {
        drop(
            read.clone()
                .poll_unpin(&mut Context::from_waker(Waker::noop())),
        );
    }
}

impl IoSource for SegmentIoSource {
    fn submit(&self, owner: IoOwnerId, batch: IoBatch) -> VortexResult<()> {
        let reads = self.reads.lock();
        let mut prefetched = self.prefetched.lock();
        // Announcements first: a request registered before a read is started is folded into
        // it by a coalescing source, one registered after waits for a read of its own.
        let (announced, batch): (Vec<_>, Vec<_>) = batch
            .into_iter()
            .partition(|request| request.intent == IoIntent::Announce);
        let mut to_start = Vec::new();
        for request in announced.into_iter().chain(batch) {
            let segment = self.segment(&request)?;
            if request.intent == IoIntent::Announce {
                // An announcement is the file's: a segment already requested for it, by any
                // session of any scan, is announced once.
                let mut ahead = self.ahead.lock();
                if ahead.started.contains_key(&segment) {
                    continue;
                }
                ahead.announced.entry(segment).or_insert_with(|| {
                    tracing::trace!(segment = *segment, "v2 io new request on announce");
                    self.segments.request(segment)
                });
                continue;
            }
            if request.intent == IoIntent::Prefetch {
                // A prefetch is for whichever session fetches the segment, like a speculated
                // read: a planner reads ahead for chunks that may run on sessions of their own.
                // Registered now and started once the whole batch is registered, so a
                // coalescing source dispatches none of them before it knows them all.
                if prefetched.contains_key(&segment) {
                    continue;
                }
                let mut ahead = self.ahead.lock();
                if !ahead.started.contains_key(&segment) {
                    drop(ahead);
                    let read = self.read(segment);
                    ahead = self.ahead.lock();
                    ahead.started.insert(segment, read.clone());
                    to_start.push(read);
                }
                continue;
            }
            // A segment stays with the session once fetched, so a later stage or morsel of the
            // split that reads it again shares the read instead of starting another once the
            // first has completed. A read started ahead for the file leaves the file's map
            // with the first session that fetches it, so the file holds only what no session
            // has taken yet.
            let bytes = prefetched
                .entry(segment)
                .or_insert_with(|| {
                    let started = self.ahead.lock().started.remove(&segment);
                    started.unwrap_or_else(|| self.read(segment))
                })
                .clone()
                .map_err(move |err| vortex_err!("read of segment {segment} failed: {err}"))
                .boxed();
            let id = request.request;
            // Registered now, polled when the session is: a fetch is not marked wanted until
            // the run has nothing left to compute and has speculated, so the reads a planner
            // speculates are registered beside it and a coalescing source folds them together.
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
        for read in &to_start {
            Self::start(read);
        }
        Ok(())
    }

    /// Starts every prefetch of the batch, shared by every session that fetches the segment,
    /// and requests every announcement without polling it, for whichever session fetches the
    /// segment first. A session that has the segment already, or speculated it before,
    /// requests nothing.
    fn speculate(&self, _owner: IoOwnerId, batch: IoBatch) -> VortexResult<()> {
        let prefetched = self.prefetched.lock();
        let mut ahead = self.ahead.lock();
        // Announcements first, so every read started here is dispatched with them registered.
        let (announced, batch): (Vec<_>, Vec<_>) = batch
            .into_iter()
            .partition(|request| request.intent == IoIntent::Announce);
        let mut to_start = Vec::new();
        for request in announced.into_iter().chain(batch) {
            if request.intent == IoIntent::Fetch {
                vortex_bail!("a speculated read is never delivered, so it cannot be a fetch");
            }
            let segment = self.segment(&request)?;
            if prefetched.contains_key(&segment) || ahead.started.contains_key(&segment) {
                continue;
            }
            if request.intent == IoIntent::Announce {
                ahead.announced.entry(segment).or_insert_with(|| {
                    tracing::trace!(segment = *segment, "v2 io speculated announce");
                    self.segments.request(segment)
                });
                continue;
            }
            tracing::trace!(segment = *segment, "v2 io speculated prefetch");
            let read = ahead
                .announced
                .remove(&segment)
                .unwrap_or_else(|| self.segments.request(segment))
                .map_err(Arc::new)
                .boxed()
                .shared();
            ahead.started.insert(segment, read.clone());
            to_start.push(read);
        }
        // Every read registered, none dispatched alone before the source knows them all.
        for read in &to_start {
            Self::start(read);
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
    use vortex_io::request::IoSource;
    use vortex_io::request::IoTarget;

    use super::SegmentIoSource;
    use super::segment_ranges;
    use super::*;
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
        let io = SegmentIoSource::new(segments, segment_ranges(&locations), Ahead::default());
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

    /// A read one session speculates is started once, taken by the first fetch of the segment
    /// from any session of the service, and from then on retained by that session alone.
    #[tokio::test]
    async fn a_speculated_read_is_taken_by_the_first_fetch() -> VortexResult<()> {
        let segments = Arc::new(CountingSegments::default());
        let location = SegmentLocation {
            offset: 0,
            length: 4,
            alignment: Alignment::none(),
        };
        let service = SegmentScanIo::new(
            Arc::clone(&segments) as _,
            segment_ranges(&[location]),
            Ahead::default(),
        );
        let (planner, chunk, other) = (service.session(), service.session(), service.session());

        planner.speculate(IoOwnerId(0), vec![request(IoIntent::Prefetch, 0)])?;
        assert_eq!(segments.0.load(Ordering::Relaxed), 1, "started at once");
        planner.speculate(IoOwnerId(0), vec![request(IoIntent::Prefetch, 1)])?;
        assert_eq!(segments.0.load(Ordering::Relaxed), 1, "speculated once");

        chunk.submit(IoOwnerId(1), vec![request(IoIntent::Fetch, 2)])?;
        let completion = std::future::poll_fn(|cx| chunk.poll_completion(cx)).await?;
        assert_eq!(completion.request, IoRequestId(2));
        assert!(completion.result.is_ok());
        assert_eq!(
            segments.0.load(Ordering::Relaxed),
            1,
            "the chunk took the read"
        );

        // The first fetch takes the read out of the file's map, so the file holds only what
        // no session has taken yet; a second session fetching the segment reads it again.
        other.submit(IoOwnerId(2), vec![request(IoIntent::Fetch, 3)])?;
        assert_eq!(
            segments.0.load(Ordering::Relaxed),
            2,
            "a second session reads again"
        );

        planner.clear();
        chunk.submit(IoOwnerId(1), vec![request(IoIntent::Fetch, 4)])?;
        assert_eq!(
            segments.0.load(Ordering::Relaxed),
            2,
            "the chunk retains its read"
        );
        assert!(
            planner
                .speculate(IoOwnerId(0), vec![request(IoIntent::Fetch, 5)])
                .is_err(),
            "a speculated fetch is refused"
        );

        // An announcement is requested once and never polled; a session's own announcement
        // adds nothing, and the first fetch takes it as its read.
        let service = SegmentScanIo::new(
            Arc::clone(&segments) as _,
            segment_ranges(&[location]),
            Ahead::default(),
        );
        let (planner, late, later) = (service.session(), service.session(), service.session());
        planner.speculate(IoOwnerId(0), vec![request(IoIntent::Announce, 7)])?;
        assert_eq!(
            segments.0.load(Ordering::Relaxed),
            3,
            "requested for coalescing"
        );
        late.submit(IoOwnerId(3), vec![request(IoIntent::Announce, 9)])?;
        assert_eq!(
            segments.0.load(Ordering::Relaxed),
            3,
            "a session announces nothing more"
        );
        late.submit(IoOwnerId(3), vec![request(IoIntent::Fetch, 8)])?;
        assert_eq!(
            segments.0.load(Ordering::Relaxed),
            3,
            "the fetch took the announcement"
        );
        later.submit(IoOwnerId(4), vec![request(IoIntent::Fetch, 10)])?;
        // The fetch took the read into its session, so a later session reads again.
        assert_eq!(
            segments.0.load(Ordering::Relaxed),
            4,
            "a later session reads again"
        );
        Ok(())
    }

    /// A fetch of a prefetched segment waits on the prefetch's read rather than reading again,
    /// only the fetch is delivered, and a later fetch of the segment shares the same read.
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
            segment_ranges(&[location]),
            Ahead::default(),
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
        assert_eq!(segments.0.load(Ordering::Relaxed), 1);

        // The fetch took the prefetched read into the session; clearing the session drops it.
        io.clear();
        io.submit(IoOwnerId(0), vec![request(IoIntent::Fetch, 3)])?;
        assert!(
            std::future::poll_fn(|cx| io.poll_completion(cx))
                .await?
                .result
                .is_ok()
        );
        assert_eq!(segments.0.load(Ordering::Relaxed), 2);
        Ok(())
    }

    #[tokio::test]
    async fn an_announcement_is_one_request_for_the_file() -> VortexResult<()> {
        let segments = Arc::new(CountingSegments::default());
        let location = SegmentLocation {
            offset: 0,
            length: 4,
            alignment: Alignment::none(),
        };
        let io = SegmentIoSource::new(
            Arc::clone(&segments) as _,
            segment_ranges(&[location]),
            Ahead::default(),
        );
        io.submit(
            IoOwnerId(0),
            vec![
                request(IoIntent::Announce, 0),
                request(IoIntent::Announce, 1),
            ],
        )?;
        assert_eq!(
            segments.0.load(Ordering::Relaxed),
            1,
            "announced once for the file"
        );
        for id in 2..5 {
            io.submit(IoOwnerId(0), vec![request(IoIntent::Fetch, id)])?;
            assert!(
                std::future::poll_fn(|cx| io.poll_completion(cx))
                    .await?
                    .result
                    .is_ok()
            );
            // The first fetch turns the announcement into the read every fetch shares.
            assert_eq!(segments.0.load(Ordering::Relaxed), 1);
        }
        Ok(())
    }
}
