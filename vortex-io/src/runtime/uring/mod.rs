// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! A thread-per-core runtime whose workers each own an io_uring.
//!
//! Every core runs one worker thread, pinned to it, which runs tasks and performs file reads on its
//! own ring.
//! Nothing is handed to other threads: no blocking pool serves the reads and no kernel worker
//! threads copy them.
//!
//! Tasks are scheduled with work stealing: a task woken on a worker runs next on that worker, the
//! way a channel's receiver should run right after its sender, a spawned task joins that worker's
//! queue, and idle workers steal from busy ones.
//!
//! [`UringFileReadAt`] reads a local file through the workers' rings. A small read goes to the ring
//! of the worker that issues it. Larger ones are spread over every worker's ring in turn, posted to
//! a worker's inbox when it is not the issuing one, so the reads of one scan are copied out of the
//! page cache by every core at once rather than by the one that drives the scan. While fewer reads
//! are in flight than there are rings, a large read is also divided into parts that idle rings
//! copy at once into one buffer. A worker submits the reads a task queued as soon as the task
//! yields, in one `io_uring_enter`, and reads that the page cache holds complete inside that call.
//! Reads land in buffers from the performing worker's pool, which return to it once the bytes are
//! dropped, so steady reading reuses resident memory.
//! Reads polled off the runtime's workers fall back to blocking positional reads.
//!
//! An idle worker parks inside `io_uring_enter`, waiting on its ring. An eventfd read is kept
//! armed on every ring, so another thread wakes a parked worker by writing to its eventfd, and a
//! read completing on the ring wakes it just the same. There is no separate reactor thread.

mod file;
mod pool;
mod reactor;

use std::cell::Cell;
use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::fence;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::JoinHandle;
use std::thread::Thread;

use async_task::Runnable;
use crossbeam_deque::Injector;
use crossbeam_deque::Steal;
use crossbeam_deque::Stealer;
use crossbeam_deque::Worker;
pub use file::UringFileReadAt;
use futures::Stream;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use vortex_error::VortexResult;
use vortex_utils::parallelism::get_available_parallelism;

use self::reactor::Reactor;
use self::reactor::ReadRequest;
use self::reactor::ReadTarget;
use self::reactor::SplitRead;
use self::reactor::WakeFd;
use crate::runtime::AbortHandle;
use crate::runtime::AbortHandleRef;
use crate::runtime::BlockingRuntime;
use crate::runtime::Executor;
use crate::runtime::Handle;
use crate::runtime::blocking_pool::BlockingPool;

/// Tasks a worker runs between checks of the global queue and its ring.
const TICKS_PER_POLL: u32 = 61;

/// Tasks a worker runs from its LIFO slot before it takes one from its queue.
const MAX_LIFO_STREAK: u32 = 3;

/// Reads at least this long are spread over the workers' rings rather than performed by the
/// worker that issues them.
const SPREAD_READ_BYTES: usize = 64 << 10;

/// Reads at least this long are divided into parts that several rings perform at once.
const SPLIT_READ_BYTES: usize = 256 << 10;

/// A divided read has at most one part for each this many bytes. Smaller parts wake more workers
/// than the copying they share out is worth.
const READ_PART_BYTES: usize = 256 << 10;

/// Submission queue entries per worker ring.
const RING_ENTRIES: u32 = 256;

/// A multi-threaded runtime driven by io_uring.
///
/// The runtime owns its worker threads, which stop when it is dropped. Tasks still queued at that
/// point are dropped without running.
pub struct UringRuntime {
    shared: Arc<Shared>,
    executor: Arc<dyn Executor>,
    threads: Vec<JoinHandle<()>>,
}

impl UringRuntime {
    /// Starts a runtime with one worker per available core.
    pub fn new() -> VortexResult<Self> {
        Self::with_workers(get_available_parallelism().unwrap_or(1).max(1))
    }

    /// Starts a runtime with `workers` worker threads, failing if the kernel refuses to create a
    /// ring, for example because io_uring is disabled.
    pub fn with_workers(workers: usize) -> VortexResult<Self> {
        let workers = workers.max(1);
        let signals = (0..workers)
            .map(|_| WakeFd::new())
            .collect::<io::Result<Vec<_>>>()?;
        let queues = (0..workers).map(|_| Worker::new_fifo()).collect::<Vec<_>>();
        let shared = Arc::new(Shared {
            injector: Injector::new(),
            stealers: queues.iter().map(Worker::stealer).collect(),
            signals,
            sleepers: AtomicUsize::new(0),
            searching: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
            blocking_pool: BlockingPool::default(),
            inboxes: (0..workers).map(|_| Injector::new()).collect(),
            next_ring: AtomicUsize::new(0),
            reads_in_flight: AtomicUsize::new(0),
        });
        // Each worker creates its own ring, since a single-issuer ring may only be entered by the
        // thread that created it, and reports whether it could.
        let (started, startup) = mpsc::channel();
        let mut threads = Vec::with_capacity(workers);
        for (index, queue) in queues.into_iter().enumerate() {
            let worker_shared = Arc::clone(&shared);
            let started = started.clone();
            let thread = std::thread::Builder::new()
                .name(format!("vortex-uring-{index}"))
                .spawn(move || {
                    pin_to_cpu(index);
                    let shared = worker_shared;
                    let reactor = match Reactor::new(RING_ENTRIES, shared.signals[index].clone()) {
                        Ok(reactor) => {
                            drop(started.send(Ok(())));
                            reactor
                        }
                        Err(err) => {
                            drop(started.send(Err(err)));
                            return;
                        }
                    };
                    WorkerContext {
                        index,
                        shared,
                        queue,
                        lifo: Cell::new(None),
                        lifo_streak: Cell::new(0),
                        reactor: RefCell::new(reactor),
                        spread: RefCell::new(Vec::new()),
                    }
                    .run()
                });
            match thread {
                Ok(thread) => threads.push(thread),
                Err(err) => {
                    stop(&shared, threads);
                    return Err(err.into());
                }
            }
        }
        drop(started);
        for result in startup.iter().take(workers) {
            if let Err(err) = result {
                stop(&shared, threads);
                return Err(err.into());
            }
        }
        let executor: Arc<dyn Executor> = Arc::new(UringExecutor {
            shared: Arc::clone(&shared),
        });
        Ok(Self {
            shared,
            executor,
            threads,
        })
    }

    /// The number of worker threads.
    pub fn workers(&self) -> usize {
        self.threads.len()
    }
}

impl Drop for UringRuntime {
    fn drop(&mut self) {
        stop(&self.shared, std::mem::take(&mut self.threads));
    }
}

/// Pins the calling thread to the `index`th of the CPUs the process may run on, wrapping around
/// when there are more workers than CPUs.
///
/// Pinned workers keep their caches and do not move onto each other's cores, which otherwise
/// happens often enough between short bursts of work to make latency vary from run to run. A
/// failure leaves the thread where the scheduler puts it.
fn pin_to_cpu(index: usize) {
    // SAFETY: an all-zero `cpu_set_t` is the empty set.
    let mut allowed: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: the kernel writes at most `size_of::<cpu_set_t>()` bytes into `allowed`.
    if unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &raw mut allowed) } != 0 {
        return;
    }
    let cpus = (0..libc::CPU_SETSIZE as usize)
        // SAFETY: `cpu` is below `CPU_SETSIZE`.
        .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &allowed) })
        .collect::<Vec<_>>();
    if cpus.is_empty() {
        return;
    }
    // SAFETY: an all-zero `cpu_set_t` is the empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: the CPU came from `allowed`, so it is below `CPU_SETSIZE`. The kernel only reads
    // `set`, and a failure leaves the thread unpinned.
    unsafe {
        libc::CPU_SET(cpus[index % cpus.len()], &mut set);
        libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &raw const set);
    }
}

/// Stops the workers and waits for them to exit.
fn stop(shared: &Shared, threads: Vec<JoinHandle<()>>) {
    shared.shutdown.store(true, Ordering::SeqCst);
    for signal in &shared.signals {
        signal.wake();
    }
    for thread in threads {
        // A worker that panicked has already reported its panic.
        drop(thread.join());
    }
    // Queued tasks hold the shared state through their wakers, so drop them to free it.
    while !shared.injector.steal().is_empty() {}
}

impl BlockingRuntime for UringRuntime {
    type BlockingIterator<'a, R: 'a> = UringBlockingIterator<'a, R>;

    fn handle(&self) -> Handle {
        Handle::new(Arc::downgrade(&self.executor))
    }

    /// Polls `fut` on the calling thread, parking it between wakeups. Tasks the future spawns run
    /// on the runtime's workers.
    fn block_on<Fut, R>(&self, fut: Fut) -> R
    where
        Fut: Future<Output = R>,
    {
        park_on(fut)
    }

    fn block_on_stream<'a, S, R>(&self, stream: S) -> Self::BlockingIterator<'a, R>
    where
        S: Stream<Item = R> + Send + 'a,
        R: Send + 'a,
    {
        UringBlockingIterator {
            stream: stream.boxed(),
        }
    }
}

/// Blocks the current thread on each item of a stream.
pub struct UringBlockingIterator<'a, T> {
    stream: BoxStream<'a, T>,
}

impl<T> Iterator for UringBlockingIterator<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        park_on(self.stream.next())
    }
}

struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn park_on<F: Future>(fut: F) -> F::Output {
    let mut fut = pin!(fut);
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(output) = fut.as_mut().poll(&mut cx) {
            return output;
        }
        std::thread::park();
    }
}

/// State shared by the runtime, its executor, and its workers.
struct Shared {
    /// Tasks scheduled from threads that are not workers of this runtime.
    injector: Injector<Runnable>,
    /// One stealer per worker queue, indexed by worker.
    stealers: Vec<Stealer<Runnable>>,
    /// One wakeup signal per worker, indexed by worker.
    signals: Vec<WakeFd>,
    /// Workers parked on their ring, or about to be.
    sleepers: AtomicUsize,
    /// Workers looking for a task to steal. While one is, there is no need to wake another.
    searching: AtomicUsize,
    shutdown: AtomicBool,
    blocking_pool: BlockingPool,
    /// Reads posted to each worker by other threads, indexed by worker.
    inboxes: Vec<Injector<ReadRequest>>,
    /// The ring the next spread read goes to.
    next_ring: AtomicUsize,
    /// Reads submitted to the rings and not yet awaited to completion.
    reads_in_flight: AtomicUsize,
}

thread_local! {
    /// The worker running on this thread, if any. Set for the worker's whole life.
    static CURRENT: Cell<*const WorkerContext> = const { Cell::new(std::ptr::null()) };
}

/// Calls `f` with the current worker if this thread is a worker of any [`UringRuntime`].
fn with_current<R>(f: impl FnOnce(&WorkerContext) -> R) -> Option<R> {
    let current = CURRENT.with(Cell::get);
    // SAFETY: the pointer is set by `WorkerContext::run` for the duration of the worker's loop,
    // on the worker's own thread, and cleared before the context is dropped.
    unsafe { current.as_ref() }.map(f)
}

/// Queues a read on the ring of one of the current runtime's workers, or hands the request back
/// when this thread is not a worker.
///
/// A read of at least [`SPREAD_READ_BYTES`] is held until the issuing task yields, and then goes
/// to the workers' rings in turn (see [`WorkerContext::dispatch_spread`]), so the reads one task
/// issues are copied by every core rather than by the one running it. A smaller read stays on the
/// current worker's ring, where it costs no hand-off.
///
/// The returned guard counts the read as in flight until it is dropped.
fn submit_read(request: ReadRequest) -> Result<ReadGuard, ReadRequest> {
    let current = CURRENT.with(Cell::get);
    // SAFETY: see `with_current`.
    let Some(worker) = (unsafe { current.as_ref() }) else {
        return Err(request);
    };
    let shared = &worker.shared;
    shared.reads_in_flight.fetch_add(1, Ordering::Relaxed);
    let guard = ReadGuard(Arc::clone(shared));
    if request.len < SPREAD_READ_BYTES || shared.inboxes.len() == 1 {
        worker.reactor.borrow_mut().read(request);
    } else {
        worker.spread.borrow_mut().push(request);
    }
    Ok(guard)
}

/// Counts a read as in flight until dropped.
struct ReadGuard(Arc<Shared>);

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.0.reads_in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Divides `request` into one part for each of `targets`, the rings that read them into one
/// buffer.
fn split_read(worker: &WorkerContext, request: ReadRequest, targets: &[usize]) {
    let ReadRequest {
        file,
        offset,
        len,
        alignment,
        target,
    } = request;
    let ReadTarget::Whole { buffer, reply } = target else {
        unreachable!("only whole reads are submitted")
    };
    let (buffer, pooled) = match buffer {
        Some(buffer) => (buffer, None),
        None => {
            let mut reactor = worker.reactor.borrow_mut();
            let (class, buffer) = reactor.take_buffer(len);
            (buffer, Some((class, reactor.pool_handle())))
        }
    };
    let part = len.div_ceil(targets.len());
    let parts = len.div_ceil(part);
    let split = Arc::new(SplitRead::new(buffer, pooled, alignment, parts, reply));
    for index in 0..parts {
        let start = index * part;
        let request = ReadRequest {
            file: Arc::clone(&file),
            offset: offset + start as u64,
            len: part.min(len - start),
            alignment,
            target: ReadTarget::Part {
                split: Arc::clone(&split),
                start,
            },
        };
        worker.post_read(targets[index], request);
    }
}

impl Shared {
    fn schedule(self: &Arc<Self>, runnable: Runnable) {
        let mut runnable = Some(runnable);
        with_current(|worker| {
            if Arc::ptr_eq(&worker.shared, self)
                && let Some(runnable) = runnable.take()
            {
                worker.schedule_local(runnable);
            }
        });
        if let Some(runnable) = runnable {
            self.injector.push(runnable);
            self.notify_one();
        }
    }

    /// Queues a newly spawned task behind the work already queued, where any worker may take it.
    fn spawn(self: &Arc<Self>, runnable: Runnable) {
        let mut runnable = Some(runnable);
        with_current(|worker| {
            if Arc::ptr_eq(&worker.shared, self)
                && let Some(runnable) = runnable.take()
            {
                worker.queue.push(runnable);
            }
        });
        if let Some(runnable) = runnable {
            self.injector.push(runnable);
        }
        self.notify_one();
    }

    /// Wakes one parked worker, unless a worker is already looking for work.
    fn notify_one(&self) {
        // Pairs with the fence a worker issues between announcing it sleeps and checking the
        // queues one last time: either the worker sees the new task, or this sees the sleeper.
        fence(Ordering::SeqCst);
        if self.searching.load(Ordering::SeqCst) > 0 || self.sleepers.load(Ordering::SeqCst) == 0 {
            return;
        }
        for signal in &self.signals {
            if signal.unpark() {
                return;
            }
        }
    }

    fn has_queued_work(&self) -> bool {
        !self.injector.is_empty() || self.stealers.iter().any(|stealer| !stealer.is_empty())
    }
}

/// The [`Executor`] handed out through the runtime's [`Handle`]s.
struct UringExecutor {
    shared: Arc<Shared>,
}

impl UringExecutor {
    fn spawn_task(&self, fut: BoxFuture<'static, ()>) -> AbortHandleRef {
        let shared = Arc::clone(&self.shared);
        // A panicking task must not take its worker down. The panic is caught into the task's
        // output, and `Handle` re-raises it on the joining side through its own channel.
        let (runnable, task) = async_task::Builder::new()
            .propagate_panic(true)
            .spawn(move |()| fut, move |runnable| shared.schedule(runnable));
        self.shared.spawn(runnable);
        Box::new(UringAbortHandle { task: Some(task) })
    }
}

impl Executor for UringExecutor {
    fn spawn(&self, fut: BoxFuture<'static, ()>) -> AbortHandleRef {
        self.spawn_task(fut)
    }

    fn spawn_cpu(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        // CPU work runs on the workers, next to the IO that feeds it.
        self.spawn_task(Box::pin(async move { task() }))
    }

    fn spawn_blocking_io(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        self.shared.blocking_pool.spawn(task)
    }
}

/// Cancels the task when aborted, and detaches it when dropped.
struct UringAbortHandle {
    task: Option<async_task::Task<()>>,
}

impl AbortHandle for UringAbortHandle {
    fn abort(mut self: Box<Self>) {
        drop(self.task.take());
    }
}

impl Drop for UringAbortHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.detach();
        }
    }
}

/// One worker thread's state. Lives on the worker's stack for the life of the thread.
struct WorkerContext {
    index: usize,
    shared: Arc<Shared>,
    queue: Worker<Runnable>,
    /// The task most recently woken by this worker, run next. A task that wakes another, as a
    /// channel send does, hands it over without a trip through the queue, so the receiver runs
    /// while the data it was sent is still in cache. Not visible to stealers.
    lifo: Cell<Option<Runnable>>,
    /// Tasks run from the LIFO slot in a row, capped so a ping-pong pair cannot starve the queue.
    lifo_streak: Cell<u32>,
    reactor: RefCell<Reactor>,
    /// Large reads the running task issued, spread over the rings once it yields.
    spread: RefCell<Vec<ReadRequest>>,
}

impl WorkerContext {
    fn run(self) {
        CURRENT.with(|current| current.set(&raw const self));
        // Clears the thread-local even if a task panics through the loop.
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                CURRENT.with(|current| current.set(std::ptr::null()));
            }
        }
        let _reset = Reset;

        let mut tick: u32 = 0;
        while !self.shared.shutdown.load(Ordering::Relaxed) {
            self.take_posted_reads();
            tick = tick.wrapping_add(1);
            if tick.is_multiple_of(TICKS_PER_POLL) {
                // Keep the ring moving even while local work never runs out, and let tasks
                // injected from outside the runtime in.
                self.reactor.borrow_mut().poll();
                if let Some(runnable) = self.steal_from_injector() {
                    self.run_task(runnable);
                    continue;
                }
            }
            if let Some(runnable) = self.next_task() {
                self.run_task(runnable);
                continue;
            }
            // Nothing runnable: flush submissions and take whatever completed.
            if self.reactor.borrow_mut().poll() > 0 {
                continue;
            }
            self.park();
        }
        // Drop queued tasks while the runtime's executor may still be alive. Their wakers only
        // hold the shared state, which outlives them.
        drop(self.lifo.take());
        while self.queue.pop().is_some() {}
        self.reactor.borrow_mut().shutdown();
    }

    /// Puts a task woken on this worker in the LIFO slot, moving the slot's previous task to the
    /// stealable queue.
    fn schedule_local(&self, runnable: Runnable) {
        if let Some(previous) = self.lifo.replace(Some(runnable)) {
            self.queue.push(previous);
            // Another worker may take what this one cannot run right now.
            self.shared.notify_one();
        }
    }

    /// Runs `runnable`, then starts the reads it issued.
    fn run_task(&self, runnable: Runnable) {
        runnable.run();
        self.dispatch_spread();
        // Start the reads the task queued now, rather than after the rest of the queue: the task,
        // or the scan it belongs to, is probably waiting on them.
        let mut reactor = self.reactor.borrow_mut();
        if reactor.has_unsubmitted() {
            reactor.poll();
        }
    }

    /// Hands the large reads the last task issued to the workers' rings in turn.
    ///
    /// While fewer reads are in flight than there are rings, a read of at least
    /// [`SPLIT_READ_BYTES`] is divided among the rings the other reads leave idle, so that a take
    /// that needs only a few large segments still has every core copying them. Every part goes to
    /// another ring in turn, like whole reads, so a task that issues many reads does not copy the
    /// first part of each itself.
    fn dispatch_spread(&self) {
        let pending = std::mem::take(&mut *self.spread.borrow_mut());
        if pending.is_empty() {
            return;
        }
        let shared = &self.shared;
        let rings = shared.inboxes.len();
        let in_flight = shared.reads_in_flight.load(Ordering::Relaxed).max(1);
        for request in pending {
            let parts = if request.len >= SPLIT_READ_BYTES {
                (rings / in_flight).clamp(1, request.len.div_ceil(READ_PART_BYTES))
            } else {
                1
            };
            let first = shared.next_ring.fetch_add(parts, Ordering::Relaxed);
            if parts > 1 {
                let targets = (0..parts)
                    .map(|offset| (first + offset) % rings)
                    .collect::<Vec<_>>();
                split_read(self, request, &targets);
            } else {
                self.post_read(first % rings, request);
            }
        }
    }

    /// Queues `request` on the ring of worker `target`: this worker's own, or another's inbox.
    fn post_read(&self, target: usize, request: ReadRequest) {
        if target == self.index {
            self.reactor.borrow_mut().read(request);
        } else {
            self.shared.inboxes[target].push(request);
            self.shared.signals[target].unpark();
        }
    }

    /// Moves the reads other threads posted to this worker onto its ring, and starts them.
    fn take_posted_reads(&self) {
        let inbox = &self.shared.inboxes[self.index];
        if inbox.is_empty() {
            return;
        }
        let mut reactor = self.reactor.borrow_mut();
        loop {
            match inbox.steal() {
                Steal::Success(request) => reactor.read(request),
                Steal::Empty => break,
                Steal::Retry => {}
            }
        }
        reactor.poll();
    }

    fn next_task(&self) -> Option<Runnable> {
        if self.lifo_streak.get() < MAX_LIFO_STREAK
            && let Some(runnable) = self.lifo.take()
        {
            self.lifo_streak.set(self.lifo_streak.get() + 1);
            return Some(runnable);
        }
        self.lifo_streak.set(0);
        if let Some(runnable) = self.queue.pop() {
            return Some(runnable);
        }
        // The streak cap only defers the slot behind the queue.
        if let Some(runnable) = self.lifo.take() {
            return Some(runnable);
        }
        self.shared.searching.fetch_add(1, Ordering::SeqCst);
        let found = self
            .steal_from_injector()
            .or_else(|| self.steal_from_peers());
        let last_searcher = self.shared.searching.fetch_sub(1, Ordering::SeqCst) == 1;
        if found.is_some() && last_searcher {
            // There may be more work than this worker can take: hand the search on.
            self.shared.notify_one();
        }
        found
    }

    fn steal_from_injector(&self) -> Option<Runnable> {
        loop {
            match self.shared.injector.steal_batch_and_pop(&self.queue) {
                Steal::Success(runnable) => return Some(runnable),
                Steal::Empty => return None,
                Steal::Retry => {}
            }
        }
    }

    fn steal_from_peers(&self) -> Option<Runnable> {
        let peers = self.shared.stealers.len();
        for offset in 1..peers {
            let stealer = &self.shared.stealers[(self.index + offset) % peers];
            loop {
                match stealer.steal_batch_and_pop(&self.queue) {
                    Steal::Success(runnable) => return Some(runnable),
                    Steal::Empty => break,
                    Steal::Retry => {}
                }
            }
        }
        None
    }

    fn has_lifo(&self) -> bool {
        let runnable = self.lifo.take();
        let has = runnable.is_some();
        self.lifo.set(runnable);
        has
    }

    fn park(&self) {
        let signal = &self.shared.signals[self.index];
        signal.set_parked(true);
        self.shared.sleepers.fetch_add(1, Ordering::SeqCst);
        // Pairs with the fence in `Shared::notify_one`.
        fence(Ordering::SeqCst);
        let runnable = !self.queue.is_empty()
            || !self.shared.inboxes[self.index].is_empty()
            || self.has_lifo()
            || self.shared.has_queued_work()
            || self.shared.shutdown.load(Ordering::SeqCst);
        if !runnable {
            self.reactor.borrow_mut().wait();
        }
        self.shared.sleepers.fetch_sub(1, Ordering::SeqCst);
        signal.set_parked(false);
    }
}

#[cfg(test)]
mod tests;
