// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! A single-worker driver for the protocol.
//!
//! A [`Run`] drives any number of root planners, each with its own IO session, and hands their
//! batches to its caller as they are produced. Runnable items are prioritised in DFS preorder,
//! with roots ordered by admission and children by emission. Each visit takes at most one CPU
//! step. A parent's continuation follows its emitted child's entire subtree. Waiting work is
//! skipped and regains its priority when IO makes it runnable. Published IO is registered
//! immediately and state is inspected again before parking. Output follows emission order,
//! not global row order. An earlier runnable branch can starve later branches.
//!
//! The run is the only part that sees the IO session. Items publish the segments they need as
//! plain requests, and the run registers them, holds the handles, and delivers each fetched
//! segment's bytes to the item that asked for it. Announcements and prefetches belong to the root:
//! the run holds them until a fetch of the same segment consumes one, a `Forget` withdraws them,
//! or the root finishes.
//!
//! A run never blocks: [`Run::advance`] returns [`Progress::Waiting`] when every live item waits
//! for IO, and the caller awaits [`Run::poll_completion`]. [`Driver::run`] is the blocking
//! convenience for one root.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use futures::future;
use vortex_array::ArrayRef;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::segments::IoIntent;
use vortex_io::segments::IoOwnerId;
use vortex_io::segments::SegmentHandle;
use vortex_io::segments::SegmentIoSession;
use vortex_io::segments::SegmentKey;
use vortex_io::segments::SegmentRequest;
use vortex_io::trace::timestamp_ns;
use vortex_utils::aliases::hash_map::Entry;
use vortex_utils::aliases::hash_map::HashMap;

use crate::planning::morsel::Morsel;
use crate::planning::morsel::MorselOutput;
use crate::planning::planner::Planner;
use crate::planning::planner::PlannerOutput;
use crate::planning::planner::State;
use crate::planning::planner::WorkScope;

mod trace;

/// Identifies one root admitted to a [`Run`]. Unique within the run, in admission order.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RootId(pub u64);

/// One array produced by a morsel, with the scope the morsel was created for.
pub struct Batch {
    /// The root the morsel descends from.
    pub root: RootId,
    /// Scope of the morsel that produced the array.
    pub scope: WorkScope,
    /// The non-empty array.
    pub array: ArrayRef,
}

/// What a call to [`Run::advance`] left behind.
pub enum Progress {
    /// A morsel's batch. One morsel's batches arrive in the order it emitted them.
    Batch(Batch),
    /// Every item descended from the root has retired, and its IO session and the reads it held
    /// were dropped.
    RootDone(RootId),
    /// Every live item waits for IO. Await [`Run::poll_completion`].
    Waiting,
    /// Nothing is live: every admitted root has finished.
    Idle,
}

enum Item {
    Planner(Box<dyn Planner>),
    Morsel(Box<dyn Morsel>),
}

/// What one item's `compute()` produced.
enum Output {
    Planner(PlannerOutput),
    Morsel(MorselOutput),
}

/// One live planner or morsel. Boxed in the run queue and the parked map, so scheduling moves a
/// pointer.
struct Work {
    id: IoOwnerId,
    root: RootId,
    scope: WorkScope,
    item: Item,
    /// Stable child-ordinal path, beginning with the root's admission ordinal.
    path: Vec<u64>,
    /// The parent continuation sorts after all previously emitted child subtrees.
    next_child: u64,
    /// Fetches not yet delivered.
    pending: Vec<(SegmentKey, SegmentHandle)>,
}

impl Work {
    fn priority(&self) -> Vec<u64> {
        let mut priority = self.path.clone();
        priority.push(self.next_child);
        priority
    }

    // Child k takes the continuation's old key as its path. Its subtree then sorts before
    // the parent's new continuation at k + 1, even if descendants are discovered after IO.
    fn child_path(&mut self) -> Vec<u64> {
        let path = self.priority();
        self.next_child += 1;
        path
    }

    fn state(&self) -> State {
        match &self.item {
            Item::Planner(planner) => planner.state(),
            Item::Morsel(morsel) => morsel.state(),
        }
    }

    /// Hands every fetched segment whose bytes arrived to the item. A failed read fails the run.
    fn deliver_ready(&mut self) -> VortexResult<()> {
        let mut index = 0;
        while index < self.pending.len() {
            let Some(result) = self.pending[index].1.try_get() else {
                index += 1;
                continue;
            };
            // Dropping the handle releases this item's hold on the bytes.
            let (key, _) = self.pending.swap_remove(index);
            let bytes: BufferHandle = result?;
            match &mut self.item {
                Item::Planner(planner) => planner.deliver(key, bytes),
                Item::Morsel(morsel) => morsel.deliver(key, bytes),
            }
        }
        Ok(())
    }
}

/// Keeps scheduling order separate from lookup for deliveries to runnable work.
#[derive(Default)]
struct ReadyQueue {
    priorities: BTreeMap<Vec<u64>, IoOwnerId>,
    work: HashMap<IoOwnerId, Box<Work>>,
}

impl ReadyQueue {
    fn insert(&mut self, work: Box<Work>) {
        self.priorities.insert(work.priority(), work.id);
        self.work.insert(work.id, work);
    }

    fn pop(&mut self) -> Option<Box<Work>> {
        let (_, owner) = self.priorities.pop_first()?;
        self.work.remove(&owner)
    }

    fn is_empty(&self) -> bool {
        self.priorities.is_empty()
    }

    fn cancel(&mut self, root: RootId) {
        self.work.retain(|_, work| work.root != root);
        self.priorities
            .retain(|_, owner| self.work.contains_key(owner));
    }
}

/// Poll IO even when one high-priority branch keeps producing CPU work.
const IO_POLL_INTERVAL: usize = 64;

/// One admitted root: the session its descendants read through, the optional reads they
/// published, and how many of them are live.
struct Root {
    io: Arc<dyn SegmentIoSession>,
    /// Announcements and prefetches, one handle per potential consumer.
    hints: HashMap<SegmentKey, Vec<SegmentHandle>>,
    live: usize,
}

/// A set of root planners advanced by its caller on one thread at a time.
///
/// Dropping a run drops every handle it holds, withdrawing the reads of every root still live.
#[derive(Default)]
pub struct Run {
    trace: Option<trace::Trace>,
    roots: HashMap<RootId, Root>,
    next_root: u64,
    next_owner: u64,
    queue: ReadyQueue,
    parked: HashMap<IoOwnerId, Box<Work>>,
    /// Batches and finished roots not yet returned by [`advance`](Self::advance).
    events: VecDeque<Progress>,
    /// Visits left before the sessions are polled for finished reads again.
    visits_until_poll: usize,
    /// Owners whose fetches finished, reused between polls.
    ready: Vec<IoOwnerId>,
    step_limit: Option<usize>,
    steps: usize,
}

impl Drop for Run {
    fn drop(&mut self) {
        if let Some(trace) = &self.trace {
            trace.event("end", self.queue.work.len(), self.parked.len());
        }
    }
}

impl Run {
    /// Creates a run with nothing admitted.
    pub fn new() -> Self {
        let mut run = Self::default();
        if let Some(trace) = trace::Trace::new() {
            run.trace = Some(trace);
        }
        run
    }

    /// Fails with an error naming the limit if more than `steps` visits are needed. Tests use it
    /// to catch loops.
    pub fn with_step_limit(mut self, steps: usize) -> Self {
        self.step_limit = Some(steps);
        self
    }

    /// Admits `root` and everything it spawns, reading through `io`.
    ///
    /// The session belongs to the root: the run submits the requests of every descendant to it,
    /// and drops it, with every handle it gave out, once they have all retired or the root is
    /// cancelled.
    pub fn admit(
        &mut self,
        root: Box<dyn Planner>,
        scope: WorkScope,
        io: Arc<dyn SegmentIoSession>,
    ) -> RootId {
        let id = RootId(self.next_root);
        self.next_root += 1;
        self.roots.insert(
            id,
            Root {
                io,
                hints: HashMap::default(),
                live: 0,
            },
        );
        if let Some(trace) = &self.trace {
            tracing::debug!(target: "vortex_scan::driver", run = trace.id,
                ts_ns = timestamp_ns(), event = "admit", root = id.0,
                file = scope.file_ordinal, row_start = scope.rows.start, row_end = scope.rows.end,
                "scan driver");
        }
        self.spawn(id, scope, Item::Planner(root), vec![id.0]);
        id
    }

    /// Holds announcements and prefetches registered for `root` before it was admitted, as if one
    /// of its items had published them.
    pub fn hold(
        &mut self,
        root: RootId,
        handles: impl IntoIterator<Item = (SegmentKey, SegmentHandle)>,
    ) {
        if let Some(root) = self.roots.get_mut(&root) {
            for (key, handle) in handles {
                root.hints.entry(key).or_default().push(handle);
            }
        }
    }

    /// Drops every live item descended from `root`, withdrawing its reads. Batches the root
    /// already produced are still returned, followed by no [`Progress::RootDone`].
    pub fn cancel(&mut self, root: RootId) {
        if self.roots.remove(&root).is_none() {
            return;
        }
        if let Some(trace) = &self.trace {
            tracing::debug!(target: "vortex_scan::driver", run = trace.id,
                ts_ns = timestamp_ns(), event = "cancel", root = root.0, "scan driver");
        }
        self.queue.cancel(root);
        self.parked.retain(|_, work| work.root != root);
    }

    /// The number of admitted roots that have not finished.
    pub fn live_roots(&self) -> usize {
        self.roots.len()
    }

    /// Runs ready work, delivering the reads that already finished, until there is something to
    /// hand back or everything left waits for IO. Never blocks.
    pub fn advance(&mut self) -> VortexResult<Progress> {
        if let Some(trace) = &self.trace {
            trace.event("advance_begin", self.queue.work.len(), self.parked.len());
        }
        let progress = self.advance_inner();
        if let Some(trace) = &self.trace {
            let event = match &progress {
                Ok(Progress::Batch(_)) => "batch",
                Ok(Progress::RootDone(_)) => "root_done",
                Ok(Progress::Waiting) => "waiting",
                Ok(Progress::Idle) => "idle",
                Err(_) => "error",
            };
            trace.event(event, self.queue.work.len(), self.parked.len());
        }
        progress
    }

    fn advance_inner(&mut self) -> VortexResult<Progress> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Ok(event);
            }
            if self.visits_until_poll == 0 || self.queue.is_empty() {
                self.take_finished()?;
                self.visits_until_poll = IO_POLL_INTERVAL;
                if self.queue.is_empty() {
                    return Ok(if self.parked.is_empty() {
                        Progress::Idle
                    } else {
                        Progress::Waiting
                    });
                }
            }
            self.visits_until_poll -= 1;
            let Some(work) = self.queue.pop() else {
                continue;
            };
            self.steps += 1;
            if let Some(limit) = self.step_limit
                && self.steps > limit
            {
                vortex_bail!("driver exceeded its step limit of {limit}");
            }
            self.visit(work)?;
        }
    }

    /// Delivers the finished fetches of `owner`, and moves it to the run queue if it no longer
    /// waits.
    fn deliver(&mut self, owner: IoOwnerId) -> VortexResult<()> {
        if let Some(trace) = &self.trace {
            tracing::debug!(target: "vortex_scan::driver", run = trace.id,
                ts_ns = timestamp_ns(), event = "completion", owner = owner.0, "scan driver");
        }
        if let Entry::Occupied(mut parked) = self.parked.entry(owner) {
            let work = parked.get_mut();
            work.deliver_ready()?;
            // An item that still waits for other fetches stays parked.
            if work.state() != State::Waiting {
                let work = parked.remove();
                if let Some(trace) = &self.trace {
                    trace.work("unpark", &work);
                }
                self.queue.insert(work);
            } else if work.pending.is_empty() {
                vortex_bail!(
                    "work for scope {:?} waits with no outstanding fetch",
                    work.scope
                );
            }
            return Ok(());
        }
        // An item that published fetches and had CPU work left is still in the run queue. An
        // item that retired or was cancelled has dropped its fetches, so nothing is delivered.
        match self.queue.work.get_mut(&owner) {
            Some(work) => work.deliver_ready(),
            None => Ok(()),
        }
    }

    /// Waits for a fetch of any root's session to finish, registering `cx` to be woken by the
    /// next one, and delivers every finished fetch.
    ///
    /// Call it after [`advance`](Self::advance) returned [`Progress::Waiting`].
    pub fn poll_completion(&mut self, cx: &mut Context<'_>) -> Poll<VortexResult<()>> {
        if self.roots.is_empty() {
            return Poll::Ready(Err(vortex_err!(
                "the run is waiting for IO with no live root"
            )));
        }
        let mut ready = std::mem::take(&mut self.ready);
        let mut in_flight = false;
        for root in self.roots.values() {
            match root.io.poll_ready(cx, &mut ready) {
                Poll::Ready(Ok(())) | Poll::Pending => in_flight = true,
                // A session with no fetch in flight has nothing to wake the run with.
                Poll::Ready(Err(_)) => {}
            }
        }
        let result = if !ready.is_empty() {
            Poll::Ready(ready.drain(..).try_for_each(|owner| self.deliver(owner)))
        } else if !in_flight {
            Poll::Ready(Err(vortex_err!(
                "the run is waiting for IO with no fetch in flight"
            )))
        } else {
            Poll::Pending
        };
        self.ready = ready;
        result
    }

    /// Delivers the fetches every root's session has finished, without blocking.
    fn take_finished(&mut self) -> VortexResult<()> {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut ready = std::mem::take(&mut self.ready);
        for root in self.roots.values() {
            // A session with nothing in flight just has nothing to report.
            drop(root.io.poll_ready(&mut cx, &mut ready));
        }
        let result = ready.drain(..).try_for_each(|owner| self.deliver(owner));
        self.ready = ready;
        result
    }

    fn spawn(&mut self, root: RootId, scope: WorkScope, item: Item, path: Vec<u64>) {
        let id = IoOwnerId(self.next_owner);
        self.next_owner += 1;
        if let Some(root) = self.roots.get_mut(&root) {
            root.live += 1;
        }
        let work = Box::new(Work {
            id,
            root,
            scope,
            item,
            path,
            next_child: 0,
            pending: Vec::new(),
        });
        if let Some(trace) = &self.trace {
            trace.work("spawn", &work);
        }
        self.queue.insert(work);
    }

    /// Retires a finished item, and its root once nothing descended from it is live.
    fn retire(&mut self, work: &Work) {
        if let Some(trace) = &self.trace {
            trace.work("retire", work);
        }
        let Entry::Occupied(mut root) = self.roots.entry(work.root) else {
            return;
        };
        root.get_mut().live -= 1;
        if root.get().live == 0 {
            // Dropping the root drops its session and every optional read it still holds.
            root.remove();
            self.events.push_back(Progress::RootDone(work.root));
        }
    }

    /// Parks an item that waits for IO, which is a protocol error with no fetch outstanding.
    fn park(&mut self, work: Box<Work>) -> VortexResult<()> {
        if work.pending.is_empty() {
            vortex_bail!(
                "work for scope {:?} waits with no outstanding fetch",
                work.scope
            );
        }
        if let Some(trace) = &self.trace {
            trace.work("park", &work);
        }
        self.parked.insert(work.id, work);
        Ok(())
    }

    /// Requeues, parks, or retires an item according to the state it reports.
    fn settle(&mut self, work: Box<Work>) -> VortexResult<()> {
        match work.state() {
            State::Done => self.retire(&work),
            State::NeedsCompute => self.queue.insert(work),
            State::Waiting => self.park(work)?,
        }
        Ok(())
    }

    /// Visits one item: computes once if it is ready, and settles it otherwise.
    fn visit(&mut self, mut work: Box<Work>) -> VortexResult<()> {
        let started =
            tracing::enabled!(target: "vortex_scan::compute_timing", tracing::Level::DEBUG)
                .then(Instant::now);
        let selection = self.trace.as_ref().and_then(|_| match &work.item {
            Item::Planner(planner) => planner.trace_selection(),
            Item::Morsel(_) => None,
        });
        let start = self.trace.as_ref().map(|_| timestamp_ns());
        let output = match &mut work.item {
            Item::Planner(planner) => match planner.state() {
                State::NeedsCompute => Output::Planner(planner.compute()?),
                _ => return self.settle(work),
            },
            Item::Morsel(morsel) => match morsel.state() {
                State::NeedsCompute => Output::Morsel(morsel.compute()?),
                _ => return self.settle(work),
            },
        };
        if let Some(started) = started {
            let compute_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            tracing::debug!(
                target: "vortex_scan::compute_timing",
                completed_unix_ns = u64::try_from(
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos()
                ).unwrap_or(u64::MAX),
                compute_ns,
                "scan compute step"
            );
        }
        if let Some(trace) = &self.trace {
            trace.compute(&work, start.unwrap_or_default(), &output);
            if let (Some((revision, before_rows)), Item::Planner(planner)) = (selection, &work.item)
                && let Some((after_revision, selected_rows)) = planner.trace_selection()
                && revision != after_revision
            {
                tracing::debug!(target: "vortex_scan::driver", run = trace.id,
                    ts_ns = timestamp_ns(), event = "selection", owner = work.id.0,
                    root = work.root.0, revision = after_revision, before_rows, selected_rows,
                    stage = planner.trace_name(), "scan driver");
            }
        }
        match output {
            Output::Planner(PlannerOutput::Done) | Output::Morsel(MorselOutput::Done) => {
                self.retire(&work);
            }
            Output::Planner(PlannerOutput::Continue) | Output::Morsel(MorselOutput::Continue) => {
                self.queue.insert(work);
            }
            Output::Planner(PlannerOutput::NeedsIO(batch))
            | Output::Morsel(MorselOutput::NeedsIO(batch)) => {
                self.submit(&mut work, batch)?;
                self.settle(work)?;
            }
            Output::Planner(PlannerOutput::Planner(scope, child)) => {
                let path = work.child_path();
                self.spawn(work.root, scope, Item::Planner(child), path);
                self.queue.insert(work);
            }
            Output::Planner(PlannerOutput::Morsel(scope, morsel)) => {
                let path = work.child_path();
                self.spawn(work.root, scope, Item::Morsel(morsel), path);
                self.queue.insert(work);
            }
            Output::Morsel(MorselOutput::Batch(array)) => {
                if array.is_empty() {
                    vortex_bail!("morsel for scope {:?} returned an empty batch", work.scope);
                }
                self.events.push_back(Progress::Batch(Batch {
                    root: work.root,
                    scope: work.scope.clone(),
                    array,
                }));
                self.queue.insert(work);
            }
        }
        Ok(())
    }

    /// Registers the requests an item published with its root's session, keeps their handles,
    /// and delivers any fetch whose bytes are already there.
    fn submit(&mut self, work: &mut Work, batch: Vec<SegmentRequest>) -> VortexResult<()> {
        if batch.is_empty() {
            vortex_bail!("compute() published an empty batch");
        }
        let Some(root) = self.roots.get_mut(&work.root) else {
            vortex_bail!("work {:?} outlived its root {:?}", work.id, work.root);
        };
        let mut registered = Vec::with_capacity(batch.len());
        for request in &batch {
            if let Some(trace) = &self.trace {
                trace.request(work, request);
            }
            if request.intent == IoIntent::Fetch
                && (work.pending.iter().any(|(key, _)| *key == request.key)
                    || registered.iter().any(|other: &SegmentRequest| {
                        other.intent == IoIntent::Fetch && other.key == request.key
                    }))
            {
                vortex_bail!(
                    "fetch of {:?} by {:?} was published while it is outstanding",
                    request.key,
                    work.id
                );
            }
            if request.intent != IoIntent::Forget {
                registered.push(*request);
            }
        }
        // Withdraw the hints a `Forget` names before the batch's reads are registered, so the
        // service never coalesces with them.
        for request in &batch {
            if request.intent == IoIntent::Forget {
                root.hints.remove(&request.key);
            }
        }
        let mut handles = if registered.is_empty() {
            Vec::new()
        } else {
            root.io.submit(work.id, &registered)?
        };
        if handles.len() != registered.len() {
            vortex_bail!(
                "the IO session returned {} handles for {} requests",
                handles.len(),
                registered.len()
            );
        }
        // Apply the batch in order, so a `Forget` also withdraws the hints published before it in
        // this batch, and keeps those published after it.
        let mut handles = handles.drain(..);
        for request in &batch {
            let key = request.key;
            match request.intent {
                IoIntent::Forget => {
                    root.hints.remove(&key);
                }
                IoIntent::Fetch => {
                    let handle = handles
                        .next()
                        .ok_or_else(|| vortex_err!("missing handle"))?;
                    // The fetch consumes one consumer the root announced, so a later fetch of
                    // the segment registers a fresh read rather than keeping these bytes.
                    if let Some(hints) = root.hints.get_mut(&key) {
                        hints.pop();
                        if hints.is_empty() {
                            root.hints.remove(&key);
                        }
                    }
                    work.pending.push((key, handle));
                }
                IoIntent::Announce => {
                    let handle = handles
                        .next()
                        .ok_or_else(|| vortex_err!("missing handle"))?;
                    root.hints.entry(key).or_default().push(handle);
                }
                IoIntent::Prefetch => {
                    let handle = handles
                        .next()
                        .ok_or_else(|| vortex_err!("missing handle"))?;
                    let hints = root.hints.entry(key).or_default();
                    if hints.is_empty() {
                        hints.push(handle);
                    }
                }
            }
        }
        work.deliver_ready()
    }
}

/// Drives one root to completion on the calling thread, blocking while it waits for IO.
pub struct Driver {
    io: Arc<dyn SegmentIoSession>,
    step_limit: Option<usize>,
}

impl Driver {
    /// Creates a driver that submits every request to `io`.
    pub fn new(io: Arc<dyn SegmentIoSession>) -> Self {
        Self {
            io,
            step_limit: None,
        }
    }

    /// Fails with an error naming the limit if more than `steps` visits are needed. Tests use it
    /// to catch loops.
    pub fn with_step_limit(self, steps: usize) -> Self {
        Self {
            step_limit: Some(steps),
            ..self
        }
    }

    /// Runs `root` and everything it spawns, returning batches in emission order.
    pub fn run(&self, root: Box<dyn Planner>) -> VortexResult<Vec<Batch>> {
        let mut run = Run::new();
        run.step_limit = self.step_limit;
        let scope = WorkScope {
            file_ordinal: 0,
            rows: 0..0,
        };
        run.admit(root, scope, Arc::clone(&self.io));
        let mut batches = Vec::new();
        loop {
            match run.advance()? {
                Progress::Batch(batch) => batches.push(batch),
                Progress::RootDone(_) | Progress::Idle => return Ok(batches),
                Progress::Waiting => {
                    futures::executor::block_on(future::poll_fn(|cx| run.poll_completion(cx)))?
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
