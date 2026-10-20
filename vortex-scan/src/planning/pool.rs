// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! A pool of workers that run the units a single root planner hands out.
//!
//! The root, such as a file planner, is admitted with [`Run::admit_distributing`] to a run of its
//! own, and each step of it hands out at most one unit. Any worker may step it, and the units it
//! hands out wait in a shared queue until a spare worker claims one and admits it to its own
//! [`Run`]. Everything a unit emits stays on the run that claimed it, so a unit's planners and
//! morsels, and the arrays they decode, stay with one worker.
//!
//! A worker steps the root before its own work whenever fewer units wait than workers are
//! parked, so a unit is handed out as soon as some worker can take it, while the root goes on
//! to plan the next. Workers are spawned as units are handed out with no worker parked to take
//! them, up to a limit, and a worker claims more units while the ones it holds wait for IO.
//!
//! At most a fixed number of units are handed out and not yet consumed, so the root stops
//! planning while the consumer falls behind.

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use futures::Stream;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::request::IoService;
use vortex_utils::aliases::hash_map::HashMap;

use crate::planning::driver::Child;
use crate::planning::driver::Progress;
use crate::planning::driver::RootId;
use crate::planning::driver::Run;
use crate::planning::planner::Planner;
use crate::planning::planner::WorkScope;

/// Runs a worker future, typically by spawning it on a runtime.
pub type Spawn = Arc<dyn Fn(BoxFuture<'static, ()>) + Send + Sync>;

/// Maps each batch to the pool's output, on the worker that produced it.
pub type MapFn<T> = Arc<dyn Fn(ArrayRef) -> VortexResult<T> + Send + Sync>;

/// How a [`Pool`] runs its units.
#[derive(Clone, Copy, Debug)]
pub struct PoolOptions {
    /// The most workers the pool spawns.
    pub max_workers: usize,
    /// The most units one worker runs at once.
    pub units_per_worker: usize,
    /// Whether output follows the order the root handed out its units in. Otherwise each batch
    /// is yielded as soon as it is produced.
    pub ordered: bool,
}

/// Runs the units a root planner hands out on a pool of workers. See the [module](self) docs.
pub struct Pool;

impl Pool {
    /// Admits `root` and returns the stream of its units' mapped batches.
    ///
    /// The first worker is spawned now. Within a unit, ordered output is sorted by the start of
    /// each batch's scope, since a unit's morsels finish in whatever order their reads arrive.
    /// Dropping the stream stops the workers.
    pub fn stream<T: Send + 'static>(
        root: Box<dyn Planner>,
        scope: WorkScope,
        io: Arc<dyn IoService>,
        map: MapFn<T>,
        spawn: Spawn,
        options: PoolOptions,
    ) -> PoolStream<T> {
        let max_workers = options.max_workers.max(1);
        let units_per_worker = options.units_per_worker.max(1);
        let mut file = Run::new();
        file.admit_distributing(root, scope, io.session());
        let shared = Arc::new(Shared {
            io,
            map,
            spawn,
            max_workers,
            units_per_worker,
            max_in_flight: max_workers * units_per_worker,
            state: Mutex::new(State {
                file: Some(file),
                file_waiting: false,
                file_done: false,
                pending: VecDeque::new(),
                next_seq: 0,
                in_flight: 0,
                workers: 0,
                live_workers: 0,
                parked: Vec::new(),
                stopped: false,
                events: VecDeque::new(),
                consumer: None,
            }),
        });
        shared.spawn_worker(&mut shared.state.lock());
        PoolStream {
            shared,
            ordered: options.ordered,
            units: None,
            next_unit: 0,
            buffered: BTreeMap::new(),
            ready: VecDeque::new(),
        }
    }
}

/// A unit handed out by the root and not yet claimed.
struct Unit {
    /// The unit's position in the order the root handed out its units.
    seq: u64,
    scope: WorkScope,
    child: Child,
}

/// What workers report to the stream.
enum Event<T> {
    /// A mapped batch of a unit, with the start of the scope of the morsel that produced it.
    Batch {
        seq: u64,
        start: u64,
        value: T,
    },
    /// Every batch of the unit has been reported.
    UnitDone(u64),
    /// The root has finished, having handed out this many units.
    RootDone(u64),
    Error(VortexError),
}

struct Shared<T> {
    io: Arc<dyn IoService>,
    map: MapFn<T>,
    spawn: Spawn,
    max_workers: usize,
    units_per_worker: usize,
    max_in_flight: usize,
    state: Mutex<State<T>>,
}

struct State<T> {
    /// The root's run, while no worker steps or polls it.
    file: Option<Run>,
    /// Whether the root waits for IO.
    file_waiting: bool,
    file_done: bool,
    pending: VecDeque<Unit>,
    next_seq: u64,
    /// Units handed out and not yet consumed.
    in_flight: usize,
    /// Workers spawned, and those that have not exited.
    workers: usize,
    live_workers: usize,
    /// Wakers of parked workers.
    parked: Vec<Waker>,
    /// Set by an error or by dropping the stream.
    stopped: bool,
    events: VecDeque<Event<T>>,
    consumer: Option<Waker>,
}

impl<T> State<T> {
    /// Whether the root's run can be stepped now.
    fn file_ready(&self, max_in_flight: usize) -> bool {
        self.file.is_some() && !self.file_waiting && self.in_flight < max_in_flight
    }

    fn wake_workers(&mut self) {
        for waker in self.parked.drain(..) {
            waker.wake();
        }
    }

    fn report(&mut self, event: Event<T>) {
        self.events.push_back(event);
        if let Some(consumer) = self.consumer.take() {
            consumer.wake();
        }
    }
}

impl<T: Send + 'static> Shared<T> {
    fn spawn_worker(self: &Arc<Self>, state: &mut State<T>) {
        state.workers += 1;
        state.live_workers += 1;
        let shared = Arc::clone(self);
        (self.spawn)(Box::pin(async move {
            let exit = WorkerExit(Arc::clone(&shared));
            if let Err(error) = Worker::new(&shared).run().await {
                shared.fail(error);
            }
            drop(exit);
        }));
    }

    fn fail(&self, error: VortexError) {
        let mut state = self.state.lock();
        if !state.stopped {
            state.stopped = true;
            state.report(Event::Error(error));
        }
        state.wake_workers();
    }

    /// Takes the root's run to step it: whenever it is ready if `always`, and otherwise only
    /// while fewer units wait than workers are parked.
    fn take_file(&self, always: bool) -> Option<Run> {
        let mut state = self.state.lock();
        if state.stopped || !state.file_ready(self.max_in_flight) {
            return None;
        }
        if !always && state.pending.len() > state.parked.len() {
            return None;
        }
        state.file.take()
    }

    /// Steps the root's run until it hands out a unit, waits, or finishes.
    fn step_file(self: &Arc<Self>, mut file: Run) -> VortexResult<()> {
        let progress = file.advance();
        let mut state = self.state.lock();
        match progress? {
            Progress::Handoff(handoff) => {
                let seq = state.next_seq;
                state.next_seq += 1;
                state.in_flight += 1;
                state.pending.push_back(Unit {
                    seq,
                    scope: handoff.scope,
                    child: handoff.child,
                });
                state.file = Some(file);
                if state.pending.len() > state.parked.len() && state.workers < self.max_workers {
                    self.spawn_worker(&mut state);
                }
            }
            Progress::Waiting => {
                state.file_waiting = true;
                state.file = Some(file);
            }
            Progress::RootDone(_) | Progress::Idle => {
                state.file_done = true;
                let units = state.next_seq;
                state.report(Event::RootDone(units));
            }
            Progress::Batch(_) => vortex_bail!("the pool's root produced a batch"),
        }
        state.wake_workers();
        Ok(())
    }

    fn claim(&self) -> Option<Unit> {
        let mut state = self.state.lock();
        if state.stopped {
            return None;
        }
        state.pending.pop_front()
    }
}

impl<T> Shared<T> {
    fn release(&self, units: usize) {
        let mut state = self.state.lock();
        state.in_flight -= units;
        state.wake_workers();
    }
}

/// Reports a worker's exit, including by panic, so the stream ends once every worker has.
struct WorkerExit<T>(Arc<Shared<T>>);

impl<T> Drop for WorkerExit<T> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock();
        state.live_workers -= 1;
        if let Some(consumer) = state.consumer.take() {
            consumer.wake();
        }
    }
}

/// One worker's run, holding the units it claimed.
struct Worker<'a, T> {
    shared: &'a Arc<Shared<T>>,
    run: Run,
    seqs: HashMap<RootId, u64>,
}

impl<'a, T: Send + 'static> Worker<'a, T> {
    fn new(shared: &'a Arc<Shared<T>>) -> Self {
        Self {
            shared,
            run: Run::new(),
            seqs: HashMap::default(),
        }
    }

    async fn run(mut self) -> VortexResult<()> {
        loop {
            if self.shared.state.lock().stopped {
                return Ok(());
            }
            if let Some(file) = self.shared.take_file(false) {
                self.shared.step_file(file)?;
                continue;
            }
            if self.run.live_roots() > 0 {
                match self.run.advance()? {
                    Progress::Batch(batch) => {
                        let seq = self.seq(batch.root)?;
                        let value = (self.shared.map)(batch.array)?;
                        self.shared.state.lock().report(Event::Batch {
                            seq,
                            start: batch.scope.rows.start,
                            value,
                        });
                        continue;
                    }
                    Progress::RootDone(root) => {
                        let seq = self.seq(root)?;
                        self.seqs.remove(&root);
                        self.shared.state.lock().report(Event::UnitDone(seq));
                        continue;
                    }
                    Progress::Handoff(_) => vortex_bail!("a unit cannot hand off children"),
                    Progress::Waiting | Progress::Idle => {}
                }
            }
            if self.run.live_roots() < self.shared.units_per_worker
                && let Some(unit) = self.shared.claim()
            {
                let root = self
                    .run
                    .admit_child(unit.child, unit.scope, self.shared.io.session());
                self.seqs.insert(root, unit.seq);
                continue;
            }
            if let Some(file) = self.shared.take_file(true) {
                self.shared.step_file(file)?;
                continue;
            }
            if self.run.live_roots() == 0 {
                let state = self.shared.state.lock();
                if state.file_done && state.pending.is_empty() {
                    return Ok(());
                }
            }
            future::poll_fn(|cx| self.park(cx)).await?;
        }
    }

    fn seq(&self, root: RootId) -> VortexResult<u64> {
        self.seqs
            .get(&root)
            .copied()
            .ok_or_else(|| vortex_err!("no unit for {root:?}"))
    }

    /// Waits until there is something to do: a unit to claim, the root to step, a delivery to
    /// the worker's own run or to the root's, or the pool to stop or finish.
    fn park(&mut self, cx: &mut Context<'_>) -> Poll<VortexResult<()>> {
        let poll_file = {
            let mut state = self.shared.state.lock();
            let can_claim =
                !state.pending.is_empty() && self.run.live_roots() < self.shared.units_per_worker;
            let finished = state.file_done && state.pending.is_empty();
            if state.stopped
                || can_claim
                || state.file_ready(self.shared.max_in_flight)
                || (finished && self.run.live_roots() == 0)
            {
                return Poll::Ready(Ok(()));
            }
            if !state.parked.iter().any(|waker| waker.will_wake(cx.waker())) {
                state.parked.push(cx.waker().clone());
            }
            if state.file_waiting {
                state.file.take()
            } else {
                None
            }
        };
        if let Some(mut file) = poll_file {
            let delivered = file.poll_completion(cx);
            let mut state = self.shared.state.lock();
            state.file = Some(file);
            if delivered.is_ready() {
                state.file_waiting = false;
                state.wake_workers();
                return delivered;
            }
        }
        if self.run.live_roots() > 0 {
            return self.run.poll_completion(cx);
        }
        Poll::Pending
    }
}

/// The mapped batches of a [`Pool`]'s units.
pub struct PoolStream<T> {
    shared: Arc<Shared<T>>,
    ordered: bool,
    /// How many units the root handed out, once it has finished.
    units: Option<u64>,
    /// Units fully consumed, in order, when ordered; otherwise units finished.
    next_unit: u64,
    /// Batches of units not yet yielded, and whether each unit is done.
    buffered: BTreeMap<u64, (Vec<(u64, T)>, bool)>,
    ready: VecDeque<T>,
}

impl<T> PoolStream<T> {
    fn finished(&self) -> bool {
        self.units == Some(self.next_unit)
    }

    fn handle(&mut self, event: Event<T>) -> VortexResult<()> {
        match event {
            Event::Batch { seq, start, value } => {
                if self.ordered {
                    self.buffered.entry(seq).or_default().0.push((start, value));
                } else {
                    self.ready.push_back(value);
                }
            }
            Event::UnitDone(seq) => {
                if self.ordered {
                    self.buffered.entry(seq).or_default().1 = true;
                    let mut released = 0;
                    while let Some(entry) = self.buffered.first_entry()
                        && *entry.key() == self.next_unit
                        && entry.get().1
                    {
                        let (mut batches, _) = entry.remove();
                        batches.sort_by_key(|(start, _)| *start);
                        self.ready
                            .extend(batches.into_iter().map(|(_, value)| value));
                        self.next_unit += 1;
                        released += 1;
                    }
                    if released > 0 {
                        self.shared.release(released);
                    }
                } else {
                    self.next_unit += 1;
                    self.shared.release(1);
                }
            }
            Event::RootDone(units) => self.units = Some(units),
            Event::Error(error) => return Err(error),
        }
        Ok(())
    }
}

// Nothing in the stream is pinned.
impl<T> Unpin for PoolStream<T> {}

impl<T> Stream for PoolStream<T> {
    type Item = VortexResult<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        loop {
            if let Some(value) = this.ready.pop_front() {
                return Poll::Ready(Some(Ok(value)));
            }
            if this.finished() {
                return Poll::Ready(None);
            }
            let events = {
                let mut state = this.shared.state.lock();
                if state.events.is_empty() {
                    if state.stopped {
                        return Poll::Ready(None);
                    }
                    if state.live_workers == 0 {
                        state.stopped = true;
                        return Poll::Ready(Some(Err(vortex_err!(
                            "every pool worker exited before the scan finished"
                        ))));
                    }
                    state.consumer = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                std::mem::take(&mut state.events)
            };
            for event in events {
                if let Err(error) = this.handle(event) {
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
    }
}

impl<T> Drop for PoolStream<T> {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock();
        state.stopped = true;
        state.wake_workers();
    }
}

#[cfg(test)]
mod tests;
