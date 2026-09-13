// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use futures::TryFutureExt;
use futures::future::BoxFuture;
use futures::future::Shared;
use futures::future::WeakShared;
use parking_lot::Mutex;
use vortex_array::buffer::BufferHandle;
use vortex_error::SharedVortexResult;
use vortex_error::VortexError;
use vortex_error::VortexExpect;
use vortex_io::ReadAtNowait;
use vortex_utils::aliases::dash_map::DashMap;
use vortex_utils::aliases::dash_map::Entry;
use vortex_utils::aliases::hash_map::HashMap;
use vortex_utils::aliases::hash_set::HashSet;

use crate::segments::SegmentFuture;
use crate::segments::SegmentId;
use crate::segments::SegmentSource;

/// A [`SegmentSource`] that allows multiple requesters to await the same underlying segment
/// request.
pub struct SharedSegmentSource<S> {
    inner: S,
    in_flight: Arc<DashMap<SegmentId, InFlightEntry>>,
    request_lock: Mutex<()>,
    cleanup: Option<CleanupState>,
}

struct CleanupState {
    next_generation: AtomicU64,
    max_stale_entries: usize,
    next_sweep_at: AtomicUsize,
}

type SharedSegmentFuture = BoxFuture<'static, SharedVortexResult<BufferHandle>>;
type StrongSharedSegmentFuture = Shared<SharedSegmentFuture>;

struct InFlightEntry {
    generation: u64,
    future: WeakShared<SharedSegmentFuture>,
}

impl<S: SegmentSource> SharedSegmentSource<S> {
    /// Create a new `SharedSegmentSource` wrapping the provided inner source.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            in_flight: Arc::new(DashMap::default()),
            request_lock: Mutex::new(()),
            cleanup: None,
        }
    }

    /// Create a shared source that retains at most `max_stale_entries` canceled request keys
    /// beyond the caller's live request window.
    pub fn new_with_max_stale_entries(inner: S, max_stale_entries: usize) -> Self {
        let max_stale_entries = max_stale_entries.max(1);
        Self {
            inner,
            in_flight: Arc::new(DashMap::default()),
            request_lock: Mutex::new(()),
            cleanup: Some(CleanupState {
                next_generation: AtomicU64::new(0),
                max_stale_entries,
                next_sweep_at: AtomicUsize::new(max_stale_entries),
            }),
        }
    }
}

impl<S: SegmentSource> SegmentSource for SharedSegmentSource<S> {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let _guard = self.request_lock.lock();
        self.request_with(id, |source, id| source.request(id))
    }

    fn request_background(&self, id: SegmentId) -> SegmentFuture {
        let _guard = self.request_lock.lock();
        self.request_with(id, |source, id| source.request_background(id))
    }

    fn request_background_batch(&self, ids: &[SegmentId]) -> Vec<SegmentFuture> {
        let _guard = self.request_lock.lock();
        self.maybe_remove_dropped_requests();
        let mut shared = HashMap::<SegmentId, StrongSharedSegmentFuture>::default();
        let mut missing = Vec::new();
        let mut missing_ids = HashSet::<SegmentId>::default();

        for &id in ids {
            if shared.contains_key(&id) {
                continue;
            }
            loop {
                match self.in_flight.entry(id) {
                    Entry::Occupied(entry) => {
                        if let Some(future) = entry.get().future.upgrade() {
                            shared.insert(id, future);
                            break;
                        }
                        entry.remove();
                    }
                    Entry::Vacant(_) => {
                        if missing_ids.insert(id) {
                            missing.push(id);
                        }
                        break;
                    }
                }
            }
        }

        let delegates = self.inner.request_background_batch(&missing);
        assert_eq!(
            delegates.len(),
            missing.len(),
            "SegmentSource::request_background_batch must return one future per ID"
        );
        for (id, delegate) in missing.into_iter().zip(delegates) {
            let (generation, future) = self.shared_future(id, delegate);
            self.in_flight.insert(
                id,
                InFlightEntry {
                    generation,
                    future: future
                        .downgrade()
                        .vortex_expect("just created, cannot be polled to completion"),
                },
            );
            shared.insert(id, future);
        }

        ids.iter()
            .map(|id| shared[id].clone().map_err(VortexError::from).boxed())
            .collect()
    }

    fn request_nowait(&self, id: SegmentId) -> vortex_error::VortexResult<ReadAtNowait> {
        self.inner.request_nowait(id)
    }

    fn prefers_background_reads(&self) -> bool {
        self.inner.prefers_background_reads()
    }
}

impl<S: SegmentSource> SharedSegmentSource<S> {
    fn request_with(
        &self,
        id: SegmentId,
        request: impl Fn(&S, SegmentId) -> SegmentFuture,
    ) -> SegmentFuture {
        self.maybe_remove_dropped_requests();
        loop {
            match self.in_flight.entry(id) {
                Entry::Occupied(e) => {
                    if let Some(shared_future) = e.get().future.upgrade() {
                        return shared_future.map_err(VortexError::from).boxed();
                    } else {
                        // The future has been dropped, remove the entry and try again.
                        e.remove();
                    }
                }
                Entry::Vacant(e) => {
                    let (generation, future) = self.shared_future(id, request(&self.inner, id));
                    e.insert(InFlightEntry {
                        generation,
                        future: future
                            .downgrade()
                            .vortex_expect("just created, cannot be polled to completion"),
                    });
                    return future.map_err(VortexError::from).boxed();
                }
            }
        }
    }

    fn shared_future(
        &self,
        id: SegmentId,
        delegate: SegmentFuture,
    ) -> (u64, StrongSharedSegmentFuture) {
        let Some(cleanup) = &self.cleanup else {
            return (0, delegate.map_err(Arc::new).boxed().shared());
        };
        let generation = cleanup.next_generation.fetch_add(1, Ordering::Relaxed);
        let in_flight = Arc::clone(&self.in_flight);
        let future = async move {
            let result = delegate.await.map_err(Arc::new);
            if let Entry::Occupied(entry) = in_flight.entry(id)
                && entry.get().generation == generation
            {
                entry.remove();
            }
            result
        }
        .boxed()
        .shared();
        (generation, future)
    }

    /// Completed requests remove themselves. Fully canceled requests never run that cleanup, so
    /// periodically sweep their dead weak handles. Sweeping after another fixed-size tranche is
    /// added keeps the work amortized and bounds the map by the live window plus that tranche.
    fn maybe_remove_dropped_requests(&self) {
        let Some(cleanup) = &self.cleanup else {
            return;
        };
        if self.in_flight.len() < cleanup.next_sweep_at.load(Ordering::Relaxed) {
            return;
        }
        self.in_flight
            .retain(|_, entry| entry.future.upgrade().is_some());
        cleanup.next_sweep_at.store(
            self.in_flight
                .len()
                .saturating_add(cleanup.max_stale_entries),
            Ordering::Relaxed,
        );
    }

    #[cfg(test)]
    fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use futures::channel::oneshot;
    use vortex_array::buffer::BufferHandle;
    use vortex_buffer::ByteBuffer;
    use vortex_error::VortexResult;
    use vortex_error::vortex_err;

    use super::*;
    use crate::segments::SegmentSink;
    use crate::segments::TestSegments;
    use crate::sequence::SequenceId;

    // Custom source that tracks how many times a segment is requested
    #[derive(Default, Clone)]
    struct CountingSegmentSource {
        segments: TestSegments,
        request_count: Arc<AtomicUsize>,
        batch_count: Arc<AtomicUsize>,
    }

    impl SegmentSource for CountingSegmentSource {
        fn request(&self, id: SegmentId) -> SegmentFuture {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            self.segments.request(id)
        }

        fn request_background_batch(&self, ids: &[SegmentId]) -> Vec<SegmentFuture> {
            self.batch_count.fetch_add(1, Ordering::SeqCst);
            self.request_count.fetch_add(ids.len(), Ordering::SeqCst);
            ids.iter().map(|id| self.segments.request(*id)).collect()
        }
    }

    #[derive(Clone)]
    struct ControlledSegmentSource {
        request_count: Arc<AtomicUsize>,
        sender: Arc<Mutex<Option<oneshot::Sender<VortexResult<BufferHandle>>>>>,
    }

    impl SegmentSource for ControlledSegmentSource {
        fn request(&self, _id: SegmentId) -> SegmentFuture {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            let (sender, receiver) = oneshot::channel();
            *self.sender.lock() = Some(sender);
            async move {
                receiver
                    .await
                    .map_err(|_| vortex_err!("controlled request was canceled"))?
            }
            .boxed()
        }
    }

    #[derive(Clone, Default)]
    struct ErrorSegmentSource {
        request_count: Arc<AtomicUsize>,
    }

    impl SegmentSource for ErrorSegmentSource {
        fn request(&self, _id: SegmentId) -> SegmentFuture {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            futures::future::ready(Err(vortex_err!("shared source failure"))).boxed()
        }
    }

    #[derive(Clone, Default)]
    struct PendingSegmentSource {
        request_count: Arc<AtomicUsize>,
    }

    impl SegmentSource for PendingSegmentSource {
        fn request(&self, _id: SegmentId) -> SegmentFuture {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            futures::future::pending().boxed()
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

    #[tokio::test]
    async fn dropping_one_consumer_does_not_cancel_another() -> VortexResult<()> {
        let sender = Arc::new(Mutex::new(None));
        let source = ControlledSegmentSource {
            request_count: Arc::new(AtomicUsize::new(0)),
            sender: Arc::clone(&sender),
        };
        let shared_source = SharedSegmentSource::new_with_max_stale_entries(source.clone(), 4);
        let first = shared_source.request(SegmentId::from(0));
        let second = shared_source.request(SegmentId::from(0));

        drop(first);
        sender
            .lock()
            .take()
            .vortex_expect("request sender must exist")
            .send(Ok(BufferHandle::new_host(ByteBuffer::from(vec![9, 8, 7]))))
            .map_err(|_| vortex_err!("request receiver was dropped"))?;

        assert_eq!(second.await?.unwrap_host(), ByteBuffer::from(vec![9, 8, 7]));
        assert_eq!(source.request_count.load(Ordering::Relaxed), 1);
        assert_eq!(shared_source.in_flight_len(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn delayed_completion_does_not_remove_a_newer_generation() -> VortexResult<()> {
        let sender = Arc::new(Mutex::new(None));
        let source = ControlledSegmentSource {
            request_count: Arc::new(AtomicUsize::new(0)),
            sender: Arc::clone(&sender),
        };
        let shared_source = SharedSegmentSource::new_with_max_stale_entries(source, 4);
        let id = SegmentId::from(0);
        let old = shared_source.request(id);
        let old_generation = shared_source
            .in_flight
            .get(&id)
            .vortex_expect("old request entry must exist")
            .generation;

        let replacement: StrongSharedSegmentFuture = futures::future::pending().boxed().shared();
        shared_source.in_flight.insert(
            id,
            InFlightEntry {
                generation: old_generation + 1,
                future: replacement
                    .downgrade()
                    .vortex_expect("replacement future is live"),
            },
        );
        sender
            .lock()
            .take()
            .vortex_expect("request sender must exist")
            .send(Ok(BufferHandle::new_host(ByteBuffer::from(vec![1]))))
            .map_err(|_| vortex_err!("request receiver was dropped"))?;

        assert_eq!(old.await?.unwrap_host().as_ref(), &[1]);
        assert_eq!(
            shared_source
                .in_flight
                .get(&id)
                .vortex_expect("newer request entry must remain")
                .generation,
            old_generation + 1
        );
        drop(replacement);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_consumers_observe_the_same_error() {
        let source = ErrorSegmentSource::default();
        let shared_source = SharedSegmentSource::new_with_max_stale_entries(source.clone(), 4);
        let first = shared_source.request(SegmentId::from(0));
        let second = shared_source.request(SegmentId::from(0));

        let (first, second) = futures::join!(first, second);
        assert!(
            first
                .unwrap_err()
                .to_string()
                .contains("shared source failure")
        );
        assert!(
            second
                .unwrap_err()
                .to_string()
                .contains("shared source failure")
        );
        assert_eq!(source.request_count.load(Ordering::Relaxed), 1);
        assert_eq!(shared_source.in_flight_len(), 0);

        assert!(
            shared_source
                .request(SegmentId::from(0))
                .await
                .unwrap_err()
                .to_string()
                .contains("shared source failure")
        );
        assert_eq!(source.request_count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn canceled_request_entries_are_bounded_by_the_live_window() {
        let source = PendingSegmentSource::default();
        let shared_source = SharedSegmentSource::new_with_max_stale_entries(source.clone(), 4);

        for id in 0..1_000 {
            drop(shared_source.request(SegmentId::from(id)));
            assert!(shared_source.in_flight_len() <= 4);
        }

        assert_eq!(source.request_count.load(Ordering::Relaxed), 1_000);
    }

    #[tokio::test]
    async fn test_shared_source_preserves_background_batch_and_deduplicates() -> VortexResult<()> {
        let source = CountingSegmentSource::default();
        let first = ByteBuffer::from(vec![1, 2]);
        let second = ByteBuffer::from(vec![3, 4]);
        source
            .segments
            .write(SequenceId::root().downgrade(), vec![first.clone()])
            .await?;
        source
            .segments
            .write(SequenceId::root().downgrade(), vec![second.clone()])
            .await?;

        let shared_source = SharedSegmentSource::new(source.clone());
        let futures = shared_source.request_background_batch(&[
            SegmentId::from(0),
            SegmentId::from(1),
            SegmentId::from(0),
        ]);
        let results = futures::future::try_join_all(futures).await?;

        assert_eq!(results[0].clone().unwrap_host(), first);
        assert_eq!(results[1].clone().unwrap_host(), second);
        assert_eq!(results[2].clone().unwrap_host(), first);
        assert_eq!(source.batch_count.load(Ordering::Relaxed), 1);
        assert_eq!(source.request_count.load(Ordering::Relaxed), 2);
        Ok(())
    }
}
