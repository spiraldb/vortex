// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::hash::Hash;
use std::hash::Hasher;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use futures::Stream;
use futures::StreamExt;
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use vortex_array::ArrayRef;
use vortex_array::dtype::DType;
use vortex_array::stream::ArrayStream;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_utils::aliases::hash_map::HashMap;

/// A hierarchical sequence identifier that exists within a shared universe.
///
/// SequenceIds form a collision-free universe where each ID is represented as a vector
/// of indices (e.g., `[0, 1, 2]`). The API design prevents collisions by only allowing
/// new IDs to be created through controlled advancement or descent operations.
///
/// # Hierarchy and Ordering
///
/// IDs are hierarchical and lexicographically ordered:
/// - `[0]` < `[0, 0]` < `[0, 1]` < `[1]` < `[1, 0]`
/// - A parent ID like `[0, 1]` can spawn children `[0, 1, 0]`, `[0, 1, 1]`, etc.
/// - Sibling IDs are created by advancing: `[0, 0]` → `[0, 1]` → `[0, 2]`
///
/// # Drop Ordering
///
/// When a SequenceId is dropped, it may wake futures waiting for ordering guarantees.
/// The `collapse()` method leverages this to provide deterministic ordering of
/// recursively created sequence IDs.
pub struct SequenceId {
    id: Vec<usize>,
    universe: Arc<Mutex<SequenceUniverse>>,
}

impl PartialEq for SequenceId {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for SequenceId {}

impl PartialOrd for SequenceId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SequenceId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl Hash for SequenceId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl fmt::Debug for SequenceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SequenceId").field("id", &self.id).finish()
    }
}

impl SequenceId {
    /// Creates a new root sequence universe starting with ID `[0]`.
    ///
    /// Each call to `root()` creates an independent universe with no ordering
    /// guarantees between separate root instances. Within a single universe,
    /// all IDs are strictly ordered.
    pub fn root() -> SequencePointer {
        SequencePointer(SequenceId::new(vec![0], Default::default()))
    }

    /// Creates a child sequence by descending one level in the hierarchy.
    ///
    /// If this SequenceId has ID `[1, 2]`, this method creates the first child
    /// `[1, 2, 0]` and returns a `SequencePointer` that can generate siblings
    /// `[1, 2, 1]`, `[1, 2, 2]`, etc.
    ///
    /// # Ownership
    ///
    /// This method consumes `self`, as the parent ID is no longer needed once
    /// we've descended to work with its children.
    pub fn descend(self) -> SequencePointer {
        let mut id = self.id.clone();
        id.push(0);
        SequencePointer(SequenceId::new(id, Arc::clone(&self.universe)))
    }

    /// Moves this sequence into a universe of its own, nested at this sequence's position.
    ///
    /// The returned sequence has the same ID, and every sequence later derived from it is ordered
    /// exactly as it would be in this universe. Creating and dropping those sequences only locks
    /// the nested universe, so independent subtrees written concurrently, such as the columns of a
    /// wide struct, do not contend on one lock.
    ///
    /// This sequence stays active in its own universe, on behalf of the nested one, until every
    /// sequence in the nested universe has been dropped.
    pub fn nest(self) -> SequenceId {
        let id = self.id.clone();
        let nested = SequenceUniverse {
            parent: Some(self),
            ..Default::default()
        };
        SequenceId::new(id, Arc::new(Mutex::new(nested)))
    }

    /// Waits until all SequenceIds with IDs lexicographically smaller than this one are dropped.
    ///
    /// This async method provides ordering guarantees by ensuring all "prior" sequences
    /// in the universe have been dropped before returning. Combined with the collision-free
    /// API, this guarantees that for this universe no sequences lexicographically smaller than
    /// this one will ever be created again.
    ///
    /// # Ordering Guarantee
    ///
    /// Once `collapse()` returns, you can be certain that:
    /// - All sequences with smaller IDs have been dropped
    /// - No new sequences with smaller IDs can ever be created (due to collision prevention)
    ///
    /// # Use Cases
    ///
    /// This is particularly useful for ordering recursively created work:
    /// - Recursive algorithms that spawn child tasks
    /// - Ensuring deterministic processing order across concurrent operations  
    /// - Converting hierarchical sequence identifiers to linear segment identifiers
    ///
    /// # Returns
    ///
    /// The [`SequenceId`] once all other segment IDs before it have been dropped. The caller can hold
    /// onto the sequence ID essentially as a lock on future calls to [`SequenceId::collapse`]
    /// in order to perform ordered operations.
    pub async fn collapse(&mut self) {
        WaitSequenceFuture(self).await;
    }

    /// This is intentionally not pub. [SequencePointer::advance] is the only allowed way to create
    /// [SequenceId] instances
    fn new(id: Vec<usize>, universe: Arc<Mutex<SequenceUniverse>>) -> Self {
        // NOTE: This is the only place we construct a SequenceId, and
        // we immediately add it to the universe.
        let res = Self { id, universe };
        res.universe.lock().add(&res);
        res
    }
}

impl Drop for SequenceId {
    fn drop(&mut self) {
        let (waker, parent) = self.universe.lock().remove(self);
        if let Some(w) = waker {
            w.wake();
        }
        // Releasing a nested universe's position in its parent may in turn wake the parent's
        // next sequence. This happens after the nested universe's lock is released.
        drop(parent);
    }
}

/// A pointer that can advance through sibling sequence IDs.
///
/// SequencePointer is the only mechanism for creating new SequenceIds within
/// a universe.
#[derive(Debug)]
pub struct SequencePointer(SequenceId);

impl SequencePointer {
    /// Splits this pointer into two, where the second is strictly greater than the first.
    pub fn split(mut self) -> (SequencePointer, SequencePointer) {
        (self.split_off(), self)
    }

    /// Split off a pointer to appear before the current one.
    ///
    /// The current pointer is advanced to the next sibling, and we return a new pointer.
    pub fn split_off(&mut self) -> SequencePointer {
        // Advance ourselves to the next sibling, and return a new pointer to the previous one.
        self.advance().descend()
    }

    /// Like [`SequencePointer::split_off`], but the returned pointer creates its sequences in a
    /// nested universe. See [`SequenceId::nest`].
    pub fn split_off_nested(&mut self) -> SequencePointer {
        self.advance().nest().descend()
    }

    /// Advances to the next sibling sequence and returns the current one.
    ///
    /// # Ownership
    ///
    /// This method requires `&mut self` because it advances the internal state
    /// to point to the next sibling position.
    pub fn advance(&mut self) -> SequenceId {
        let mut next_id = self.0.id.clone();

        // increment x.y.z -> x.y.(z + 1)
        let last = next_id.last_mut();
        let last = last.vortex_expect("must have at least one element");
        *last += 1;
        let next_sibling = SequenceId::new(next_id, Arc::clone(&self.0.universe));
        std::mem::replace(&mut self.0, next_sibling)
    }

    /// Converts this pointer into its current SequenceId, consuming the pointer.
    ///
    /// This method is useful when you want to access the current SequenceId
    /// without advancing to the next sibling. Once downgraded, you cannot
    /// create additional siblings from this pointer.
    pub fn downgrade(self) -> SequenceId {
        self.0
    }
}

#[derive(Default)]
struct SequenceUniverse {
    active: BTreeSet<Vec<usize>>,
    wakers: HashMap<Vec<usize>, Waker>,
    /// For a nested universe, the sequence in the parent universe that holds this universe's
    /// position for as long as any sequence in this universe is active. See [`SequenceId::nest`].
    parent: Option<SequenceId>,
}

impl SequenceUniverse {
    fn add(&mut self, sequence_id: &SequenceId) {
        self.active.insert(sequence_id.id.clone());
    }

    /// Removes a sequence, returning the waker of a sequence it unblocked and, once the universe
    /// is empty, the parent sequence to drop.
    fn remove(&mut self, sequence_id: &SequenceId) -> (Option<Waker>, Option<SequenceId>) {
        let was_first = self.active.first() == Some(&sequence_id.id);
        self.active.remove(&sequence_id.id);
        // A waker registered under this sequence belongs to a nested universe waiting for this
        // sequence to come first, and that universe is now empty.
        self.wakers.remove(&sequence_id.id);
        let Some(first) = self.active.first() else {
            // last sequence finished, we must have no pending futures
            assert!(self.wakers.is_empty(), "all wakers must have been removed");
            return (None, self.parent.take());
        };
        // A waiting sequence comes first only once the sequence before it is removed, so
        // removing any other sequence cannot unblock a waiter.
        if !was_first {
            return (None, None);
        }
        (self.wakers.remove(first), None)
    }
}

struct WaitSequenceFuture<'a>(&'a mut SequenceId);

impl Future for WaitSequenceFuture<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // A sequence is first overall when it is first in its universe and, for a nested
        // universe, the parent sequence holding the universe's position is first overall. Once a
        // sequence is first in its universe it stays first until it is dropped, so each level can
        // be checked under its own lock. A level that is not yet first registers the waker there,
        // and the removal that makes it first wakes it.
        let mut universe = Arc::clone(&self.0.universe);
        let mut id = self.0.id.clone();
        loop {
            let parent = {
                let mut guard = universe.lock();
                let current_first = guard.active.first().vortex_expect(
                    "if we have a future, we must have at least one active sequence",
                );
                if *current_first != id {
                    guard.wakers.insert(id, cx.waker().clone());
                    return Poll::Pending;
                }
                guard.wakers.remove(&id);
                match &guard.parent {
                    None => return Poll::Ready(()),
                    Some(parent) => (Arc::clone(&parent.universe), parent.id.clone()),
                }
            };
            (universe, id) = parent;
        }
    }
}

/// If the future itself is dropped, we don't want to orphan the waker
impl Drop for WaitSequenceFuture<'_> {
    fn drop(&mut self) {
        self.0.universe.lock().wakers.remove(&self.0.id);
    }
}

pub trait SequentialStream: Stream<Item = VortexResult<(SequenceId, ArrayRef)>> {
    fn dtype(&self) -> &DType;
}

pub type SendableSequentialStream = Pin<Box<dyn SequentialStream + Send>>;

impl SequentialStream for SendableSequentialStream {
    fn dtype(&self) -> &DType {
        (**self).dtype()
    }
}

pub trait SequentialStreamExt: SequentialStream {
    // not named boxed to prevent clashing with StreamExt
    fn sendable(self) -> SendableSequentialStream
    where
        Self: Sized + Send + 'static,
    {
        Box::pin(self)
    }
}

impl<S: SequentialStream> SequentialStreamExt for S {}

pin_project! {
    pub struct SequentialStreamAdapter<S> {
        dtype: DType,
        #[pin]
        inner: S,
    }
}

impl<S> SequentialStreamAdapter<S> {
    pub fn new(dtype: DType, inner: S) -> Self {
        Self { dtype, inner }
    }
}

impl<S> SequentialStream for SequentialStreamAdapter<S>
where
    S: Stream<Item = VortexResult<(SequenceId, ArrayRef)>>,
{
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

impl<S> Stream for SequentialStreamAdapter<S>
where
    S: Stream<Item = VortexResult<(SequenceId, ArrayRef)>>,
{
    type Item = VortexResult<(SequenceId, ArrayRef)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let array = futures::ready!(this.inner.poll_next(cx));
        if let Some(Ok((_, array))) = array.as_ref() {
            assert_eq!(
                array.dtype(),
                this.dtype,
                "Sequential stream of {} got chunk of {}.",
                array.dtype(),
                this.dtype
            );
        }

        Poll::Ready(array)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pub trait SequentialArrayStreamExt: ArrayStream {
    /// Converts the stream to a [`SendableSequentialStream`].
    fn sequenced(self, mut pointer: SequencePointer) -> SendableSequentialStream
    where
        Self: Sized + Send + 'static,
    {
        Box::pin(SequentialStreamAdapter::new(
            self.dtype().clone(),
            StreamExt::map(self, move |item| {
                item.map(|array| (pointer.advance(), array))
            }),
        ))
    }
}

impl<S: ArrayStream> SequentialArrayStreamExt for S {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use futures::task::ArcWake;

    use super::*;

    /// A waker that records whether it was woken.
    #[derive(Default)]
    struct Flag(AtomicBool);

    impl ArcWake for Flag {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.store(true, Ordering::SeqCst);
        }
    }

    impl Flag {
        fn take(&self) -> bool {
            self.0.swap(false, Ordering::SeqCst)
        }
    }

    /// Polls a held `collapse` future once with the given flag as its waker.
    fn poll(collapse: &mut Pin<Box<impl Future<Output = ()>>>, flag: &Arc<Flag>) -> Poll<()> {
        let waker = futures::task::waker(Arc::clone(flag));
        collapse.as_mut().poll(&mut Context::from_waker(&waker))
    }

    #[test]
    fn nested_sequence_waits_for_earlier_nested_universe() {
        let (mut ptr, _eof) = SequenceId::root().split();
        let mut first = ptr.split_off_nested();
        let mut second = ptr.split_off_nested();
        let first_id = first.advance();
        let mut second_id = second.advance();

        let flag = Arc::new(Flag::default());
        let mut collapse = Box::pin(second_id.collapse());
        assert!(poll(&mut collapse, &flag).is_pending());

        drop(first_id);
        assert!(!flag.take(), "the earlier universe still holds a pointer");
        drop(first);
        assert!(
            flag.take(),
            "emptying the earlier universe must wake the waiter"
        );
        assert!(poll(&mut collapse, &flag).is_ready());
    }

    #[test]
    fn nested_sequence_does_not_wait_for_later_sequences() {
        let (mut ptr, _eof) = SequenceId::root().split();
        let mut first = ptr.split_off_nested();
        let _second = ptr.split_off_nested();
        let _later = ptr.advance();
        let mut first_id = first.advance();

        let flag = Arc::new(Flag::default());
        let mut collapse = Box::pin(first_id.collapse());
        assert!(poll(&mut collapse, &flag).is_ready());
    }

    #[test]
    fn nested_sequence_waits_for_earlier_sequence_in_parent() {
        let (mut ptr, _eof) = SequenceId::root().split();
        let earlier = ptr.advance();
        let mut nested = ptr.split_off_nested();
        let mut nested_id = nested.advance();

        let flag = Arc::new(Flag::default());
        let mut collapse = Box::pin(nested_id.collapse());
        assert!(poll(&mut collapse, &flag).is_pending());

        drop(earlier);
        assert!(flag.take());
        assert!(poll(&mut collapse, &flag).is_ready());
    }

    #[test]
    fn parent_sequence_waits_for_earlier_nested_universe() {
        let (mut ptr, _eof) = SequenceId::root().split();
        let nested = ptr.split_off_nested();
        let mut later = ptr.advance();

        let flag = Arc::new(Flag::default());
        let mut collapse = Box::pin(later.collapse());
        assert!(poll(&mut collapse, &flag).is_pending());

        drop(nested);
        assert!(flag.take());
        assert!(poll(&mut collapse, &flag).is_ready());
    }

    #[test]
    fn sequence_waits_for_universe_nested_in_earlier_universe() {
        let (mut ptr, _eof) = SequenceId::root().split();
        let mut outer = ptr.split_off_nested();
        let inner = outer.split_off_nested();
        drop(outer);
        let mut second = ptr.split_off_nested();
        let mut second_id = second.advance();

        let flag = Arc::new(Flag::default());
        let mut collapse = Box::pin(second_id.collapse());
        assert!(poll(&mut collapse, &flag).is_pending());

        drop(inner);
        assert!(
            flag.take(),
            "emptying the inner universe must release the outer one too"
        );
        assert!(poll(&mut collapse, &flag).is_ready());
    }

    #[test]
    fn dropped_collapse_does_not_leave_a_waker_behind() {
        let (mut ptr, eof) = SequenceId::root().split();
        let first = ptr.split_off_nested();
        let mut second = ptr.split_off_nested();
        let mut second_id = second.advance();

        let flag = Arc::new(Flag::default());
        let mut collapse = Box::pin(second_id.collapse());
        assert!(poll(&mut collapse, &flag).is_pending());
        drop(collapse);

        // The waiting universe empties before it ever came first, so the waker it registered for
        // its position in the parent must go with it, or the parent trips its check that no waker
        // is left registered once it empties.
        drop((second_id, second));
        drop((first, ptr, eof));
    }
}
