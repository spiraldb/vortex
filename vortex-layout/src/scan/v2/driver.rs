// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Runs a scan's splits on the thread that polls its stream.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::Stream;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;
use vortex_io::request::IoService;
use vortex_scan::planning::driver::Batch;
use vortex_scan::planning::driver::Progress;
use vortex_scan::planning::driver::RootId;
use vortex_scan::planning::driver::Run;

use crate::scan::v2::split::SplitPlan;

/// A stream that drives its splits on one planning run, on whichever thread polls it.
///
/// Up to `lookahead` splits are admitted to the run at once. The run prefers the earliest
/// admitted split, so the later ones publish and await their reads while the first decodes:
/// the lookahead is the number of splits whose reads are in flight ahead of the consumer. No
/// task is spawned, and a batch is decoded and mapped by the poll that returns it, so the scan
/// holds no more than the lookahead's reads and the batches of the splits being released.
///
/// An unordered scan returns each batch as its morsel finishes. An ordered scan holds a split's
/// batches until the split and every split admitted before it have finished, and returns them
/// in row order.
pub(super) struct InlineScan<A> {
    run: Run,
    pending: VecDeque<SplitPlan>,
    io: Arc<dyn IoService>,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    lookahead: usize,
    ordered: bool,
    /// Admitted roots in admission order, and whether each has finished.
    live: VecDeque<(RootId, bool)>,
    /// Batches of roots that have not been released, when output is ordered.
    held: BTreeMap<RootId, Vec<Batch>>,
    /// Arrays ready to be mapped and returned, in output order.
    ready: VecDeque<ArrayRef>,
    done: bool,
}

impl<A> InlineScan<A> {
    pub(super) fn new(
        splits: Vec<SplitPlan>,
        io: Arc<dyn IoService>,
        map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
        lookahead: usize,
        ordered: bool,
    ) -> Self {
        Self {
            run: Run::new(),
            pending: splits.into(),
            io,
            map_fn,
            lookahead: lookahead.max(1),
            ordered,
            live: VecDeque::new(),
            held: BTreeMap::new(),
            ready: VecDeque::new(),
            done: false,
        }
    }

    fn admit(&mut self) {
        while self.run.live_roots() < self.lookahead
            && let Some(split) = self.pending.pop_front()
        {
            let id = self.run.admit(split.root, split.scope, self.io.session());
            self.live.push_back((id, false));
        }
    }

    fn on_batch(&mut self, batch: Batch) {
        if self.ordered {
            self.held.entry(batch.root).or_default().push(batch);
        } else {
            self.ready.push_back(batch.array);
        }
    }

    /// Releases the batches of finished roots from the head of the admission order, each
    /// root's in row order.
    fn on_root_done(&mut self, root: RootId) {
        if let Some(entry) = self.live.iter_mut().find(|(id, _)| *id == root) {
            entry.1 = true;
        }
        while let Some((id, true)) = self.live.front().copied() {
            self.live.pop_front();
            if let Some(mut batches) = self.held.remove(&id) {
                batches.sort_by_key(|batch| batch.scope.rows.start);
                self.ready
                    .extend(batches.into_iter().map(|batch| batch.array));
            }
        }
    }
}

impl<A> Stream for InlineScan<A> {
    type Item = VortexResult<A>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.done {
            return Poll::Ready(None);
        }
        loop {
            if let Some(array) = this.ready.pop_front() {
                let mapped = (this.map_fn)(array);
                this.done = mapped.is_err();
                return Poll::Ready(Some(mapped));
            }
            this.admit();
            let progress = match this.run.advance() {
                Ok(progress) => progress,
                Err(error) => {
                    this.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
            };
            match progress {
                Progress::Batch(batch) => this.on_batch(batch),
                Progress::RootDone(root) => this.on_root_done(root),
                Progress::Waiting => match this.run.poll_completion(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(error)) => {
                        this.done = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Progress::Idle => {
                    if this.pending.is_empty() {
                        this.done = true;
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}
