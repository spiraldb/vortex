// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Segment reads for scans that drive their own IO.
//!
//! Requests name a segment of a registered source by [`SegmentKey`] and say how much they need it
//! with an [`IoIntent`]. A [`SegmentIoSession`] registers a whole batch of them before any can be
//! read, and returns one [`SegmentHandle`] per request. The handle is the request: its bytes
//! arrive in it, and dropping every handle of a read withdraws the interest and cancels the read
//! if nothing else wants it.
//!
//! A [`SegmentIoService`] serves every registered source. Its sessions share and coalesce reads,
//! so one session's announcement can widen another session's read.

use std::any::Any;
use std::fmt::Display;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use futures::stream::FuturesUnordered;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

/// Identifier for a single physical segment referenced by a layout.
///
/// Segment ids are local to a file or segment source. The file footer maps ids to physical offsets;
/// custom storage systems may map them to object-store keys or other random-access locations.
// TODO(ngates): should this be a `[u8]` instead? Allowing for arbitrary segment identifiers?
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(u32);

impl From<u32> for SegmentId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl TryFrom<usize> for SegmentId {
    type Error = VortexError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        Ok(Self::from(u32::try_from(value)?))
    }
}

impl Deref for SegmentId {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for SegmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SegmentId({})", self.0)
    }
}

/// A source registered with a [`SegmentIoService`], such as one opened file.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SourceId(pub u32);

/// One segment of one registered source.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentKey {
    /// The source the segment belongs to.
    pub source: SourceId,
    /// The segment within its source.
    pub segment: SegmentId,
}

impl SegmentKey {
    /// The key of `segment` in `source`.
    pub fn new(source: SourceId, segment: SegmentId) -> Self {
        Self { source, segment }
    }
}

/// How much a request needs its segment.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum IoIntent {
    /// Needed now. Fetches are read ahead of prefetches.
    Fetch,
    /// Needed soon: read it when there is capacity.
    Prefetch,
    /// May be needed: only widens nearby reads, and is never read on its own.
    Announce,
    /// No longer needed: withdraws earlier announcements and prefetches of the segment.
    Forget,
}

/// One segment a planner or morsel asks for.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct SegmentRequest {
    /// The segment.
    pub key: SegmentKey,
    /// How much it is needed.
    pub intent: IoIntent,
}

impl SegmentRequest {
    /// A request for `key` with `intent`.
    pub fn new(key: SegmentKey, intent: IoIntent) -> Self {
        Self { key, intent }
    }
}

/// Identifies the object that submitted a request, chosen by the caller.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IoOwnerId(pub u64);

/// The bytes of one segment read, shared by every handle of it.
pub type SharedBytes = Shared<BoxFuture<'static, Result<BufferHandle, Arc<VortexError>>>>;

/// One registered segment read.
///
/// Clones share the read. The read stays registered while any handle of it, from any session, is
/// alive; dropping the last one withdraws it, and cancels it if it has not finished.
#[derive(Clone)]
pub struct SegmentHandle {
    bytes: SharedBytes,
    /// Keeps the read registered with its service while the handle lives.
    _registration: Option<Arc<dyn Any + Send + Sync>>,
}

impl SegmentHandle {
    /// A handle whose bytes arrive through `bytes`, keeping `registration` alive while it lives.
    pub fn new(bytes: SharedBytes, registration: Arc<dyn Any + Send + Sync>) -> Self {
        Self {
            bytes,
            _registration: Some(registration),
        }
    }

    /// A handle that already holds its result, such as a cache hit.
    pub fn ready(result: VortexResult<BufferHandle>) -> Self {
        let bytes = futures::future::ready(result.map_err(Arc::new))
            .boxed()
            .shared();
        // Polling once stores the result, so `try_get` sees it without a waker.
        drop(bytes.clone().now_or_never());
        Self {
            bytes,
            _registration: None,
        }
    }

    /// The segment's bytes once they have arrived. Never blocks and never reads.
    pub fn try_get(&self) -> Option<VortexResult<BufferHandle>> {
        self.bytes
            .peek()
            .map(|result| result.clone().map_err(VortexError::from))
    }

    /// The shared future the bytes arrive through, for sessions that wake on it.
    pub fn bytes(&self) -> SharedBytes {
        self.bytes.clone()
    }
}

/// One scope's segment reads, such as the work descended from one root planner.
///
/// Registration does no IO and never blocks. Every read is driven by the service, so nothing has
/// to be polled for reads to progress; polling only learns which reads finished.
pub trait SegmentIoSession: Send + Sync {
    /// Registers the whole batch before any of it can be read, and returns one handle per
    /// request, in order. Fetches are tracked for [`poll_ready`](Self::poll_ready). A batch never
    /// holds a `Forget`: callers withdraw interest by dropping handles.
    fn submit(
        &self,
        owner: IoOwnerId,
        batch: &[SegmentRequest],
    ) -> VortexResult<Vec<SegmentHandle>>;

    /// Moves the owners of fetches that finished since the last call into `ready`. Returns
    /// `Pending`, registering `cx` to be woken by the next one, if none have, and fails if no
    /// fetch is in flight, since nothing would wake the caller.
    fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        ready: &mut Vec<IoOwnerId>,
    ) -> Poll<VortexResult<()>>;
}

/// Serves the segment reads of every registered source.
pub trait SegmentIoService: Send + Sync {
    /// A new session for one scope.
    fn session(&self) -> Arc<dyn SegmentIoSession>;
}

/// A source registered with a [`SegmentIoService`]: the service, the source's id in it, and the
/// registration. The service can read the source while any clone of this is alive.
#[derive(Clone)]
pub struct RegisteredSource {
    service: Arc<dyn SegmentIoService>,
    id: SourceId,
    _registration: Arc<dyn Any + Send + Sync>,
}

impl RegisteredSource {
    /// The registration of `id` with `service`, kept alive by `registration`.
    pub fn new(
        service: Arc<dyn SegmentIoService>,
        id: SourceId,
        registration: Arc<dyn Any + Send + Sync>,
    ) -> Self {
        Self {
            service,
            id,
            _registration: registration,
        }
    }

    /// The service that reads the source.
    pub fn service(&self) -> &Arc<dyn SegmentIoService> {
        &self.service
    }

    /// The source's id in its service.
    pub fn id(&self) -> SourceId {
        self.id
    }
}

/// The fetches of one session, waking the session when any of them finishes.
///
/// Sessions keep one of these to implement [`SegmentIoSession::poll_ready`].
#[derive(Default)]
pub struct FetchTracker {
    fetches: FuturesUnordered<Pin<Box<dyn Future<Output = IoOwnerId> + Send>>>,
}

impl FetchTracker {
    /// Tracks a fetch submitted by `owner`.
    pub fn track(&mut self, owner: IoOwnerId, handle: &SegmentHandle) {
        let bytes = handle.bytes();
        self.fetches.push(Box::pin(async move {
            // Errors are taken from the handle by its owner.
            drop(bytes.await);
            owner
        }));
    }

    /// Moves the owners of finished fetches into `ready`, as [`SegmentIoSession::poll_ready`].
    pub fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
        ready: &mut Vec<IoOwnerId>,
    ) -> Poll<VortexResult<()>> {
        let before = ready.len();
        loop {
            match self.fetches.poll_next_unpin(cx) {
                Poll::Ready(Some(owner)) => ready.push(owner),
                Poll::Ready(None) if ready.len() > before => return Poll::Ready(Ok(())),
                Poll::Ready(None) => {
                    return Poll::Ready(Err(vortex_err!(
                        "waiting for segment reads with no fetch in flight"
                    )));
                }
                Poll::Pending if ready.len() > before => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Drops every tracked fetch.
    pub fn clear(&mut self) {
        self.fetches.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::task::Waker;

    use futures::channel::oneshot;
    use vortex_buffer::ByteBuffer;

    use super::*;

    fn pending() -> (oneshot::Sender<BufferHandle>, SegmentHandle) {
        let (send, recv) = oneshot::channel::<BufferHandle>();
        let bytes = recv
            .map(|result| result.map_err(|_| Arc::new(vortex_err!("cancelled"))))
            .boxed()
            .shared();
        (send, SegmentHandle::new(bytes, Arc::new(())))
    }

    #[test]
    fn ready_handle_needs_no_poll() -> VortexResult<()> {
        let handle =
            SegmentHandle::ready(Ok(BufferHandle::new_host(ByteBuffer::copy_from(b"abc"))));
        let bytes = handle.try_get().ok_or_else(|| vortex_err!("not ready"))??;
        assert_eq!(bytes.len(), 3);
        Ok(())
    }

    #[test]
    fn tracker_reports_owners_as_fetches_finish() -> VortexResult<()> {
        let mut tracker = FetchTracker::default();
        let (send_a, a) = pending();
        let (_send_b, b) = pending();
        tracker.track(IoOwnerId(1), &a);
        tracker.track(IoOwnerId(2), &b);
        let mut cx = Context::from_waker(Waker::noop());
        let mut ready = Vec::new();
        assert!(tracker.poll_ready(&mut cx, &mut ready).is_pending());
        assert!(a.try_get().is_none());

        send_a
            .send(BufferHandle::new_host(ByteBuffer::copy_from(b"a")))
            .map_err(|_| vortex_err!("receiver dropped"))?;
        assert!(matches!(
            tracker.poll_ready(&mut cx, &mut ready),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(ready, vec![IoOwnerId(1)]);
        assert!(a.try_get().is_some());
        assert!(b.try_get().is_none());
        Ok(())
    }

    #[test]
    fn tracker_fails_with_nothing_in_flight() {
        let mut tracker = FetchTracker::default();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            tracker.poll_ready(&mut cx, &mut Vec::new()),
            Poll::Ready(Err(_))
        ));
    }
}
