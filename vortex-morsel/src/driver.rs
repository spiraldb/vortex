// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Affinity-owned morsel execution over one shared asynchronous IO service.
//!
//! Each worker owns one arena and at most one active morsel. The arena never crosses a thread
//! boundary. Planning registers named segment futures scan-wide. When execution reaches an exact
//! dependency, the owning worker drives the planned futures until that dependency is ready, then
//! resumes the same morsel. Output order is restored by morsel index after all workers finish.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Poll;
use std::task::Waker;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use futures::future::poll_fn;
use parking_lot::Condvar;
use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_error::VortexError;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_layout::segments::SegmentSource;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;
use vortex_utils::parallelism::get_available_parallelism;

use crate::build::ExecPlan;
use crate::build::cut_morsels;
use crate::cells::SharedCells;
use crate::io::IoKey;
use crate::io::IoPlane;
use crate::io::IoPriority;
use crate::io::IoRead;
use crate::io::IoReadPoll;
use crate::io::IoService;
use crate::node::Arena;
use crate::node::ExecPoll;
use crate::node::PlanPoll;
use crate::node::ScanCaches;
use crate::node::Wait;
use crate::node::WaitSet;
use crate::node::begin_morsel;
use crate::node::poll_execute_morsel;
use crate::node::poll_plan_morsel;
use crate::node::retire_morsel;
use crate::stats::ScanStats;

/// The morsel row ranges for a plan.
///
/// With `target_rows` of zero every natural split is a morsel boundary, which is exactly the V1
/// split set — the fair-comparison default. A larger target coalesces consecutive splits, which
/// is where the executor's ability to straddle chunk boundaries starts to pay.
pub fn morsels(plan: &ExecPlan, target_rows: u64) -> Vec<Range<u64>> {
    cut_morsels(plan.natural_splits(), target_rows)
}

fn overlapping_morsels(morsels: &[Range<u64>], range: &Range<u64>) -> usize {
    let first = morsels.partition_point(|morsel| morsel.end <= range.start);
    let end = morsels.partition_point(|morsel| morsel.start < range.end);
    end.saturating_sub(first)
}

fn demanding_morsels(
    morsels: &[Range<u64>],
    demands: Option<&[Mask]>,
    range: &Range<u64>,
) -> usize {
    let Some(demands) = demands else {
        return overlapping_morsels(morsels, range);
    };
    morsels
        .iter()
        .zip(demands)
        .filter(|(morsel, demand)| {
            let start = range.start.max(morsel.start);
            let end = range.end.min(morsel.end);
            if start >= end {
                return false;
            }
            let local_start = usize::try_from(start - morsel.start)
                .unwrap_or_else(|_| vortex_panic!("morsel demand offset exceeds usize"));
            let local_end = usize::try_from(end - morsel.start)
                .unwrap_or_else(|_| vortex_panic!("morsel demand offset exceeds usize"));
            !demand.slice(local_start..local_end).all_false()
        })
        .count()
}

/// One configured run of the morsel executor.
pub struct MorselScan {
    plan: Arc<ExecPlan>,
    segments: Arc<dyn SegmentSource>,
    session: VortexSession,
    morsels: Arc<[Range<u64>]>,
    demands: Option<Arc<[Mask]>>,
    threads: usize,
    share_decodes: bool,
    observe: bool,
    lookahead_morsels: usize,
    completion: Option<CompletionSink>,
}

/// A reusable worker pool for scans that share one execution plan.
pub struct MorselExecutor {
    plan: Arc<ExecPlan>,
    workers: ExecutorWorkers,
    threads: usize,
}

enum ExecutorWorkers {
    Inline(Mutex<Arena>),
    Pool(Arc<MorselWorkerPool>),
}

/// Receives each completed morsel's output as soon as it is retired, in completion order.
pub type CompletionSink = Arc<dyn Fn(usize, Option<ArrayRef>) + Send + Sync>;

struct WorkerRun {
    plan: Arc<ExecPlan>,
    session: VortexSession,
    morsels: Arc<[Range<u64>]>,
    demands: Option<Arc<[Mask]>>,
    io: Arc<IoService>,
    cells: SharedCells,
    dictionary_values: Box<[OnceLock<ArrayRef>]>,
    start: Instant,
    observe_timing: bool,
    observe_morsels: bool,
    lookahead_morsels: usize,
    completion: Option<CompletionSink>,
}

#[derive(Clone, Copy)]
enum TaskPhase {
    Plan,
    Execute,
}

struct LocalMorsel<'a> {
    arena: &'a mut Arena,
    worker: usize,
    io: IoPlane,
    phase: TaskPhase,
    index: usize,
    range: Range<u64>,
    demand: Mask,
    active: bool,
    morsel_started: Option<Instant>,
    morsel_io_uses_start: u64,
    morsel_io_requests_start: u64,
    morsel_io_batches_start: u64,
    morsel_io_blocks_start: u64,
    stats: ScanStats,
}

struct Scheduler {
    run: Arc<WorkerRun>,
    workers: usize,
    sort_reads_by_segment: bool,
    reads: Mutex<HashMap<IoKey, IoRead>>,
    planning_waves: Mutex<BTreeMap<usize, PlanningWave>>,
    planning_ready: Condvar,
    blocked_workers: Mutex<Vec<Waker>>,
    results: Mutex<Vec<(usize, ArrayRef)>>,
    error: Mutex<Option<VortexError>>,
    next_morsel: AtomicUsize,
    remaining: AtomicUsize,
    /// Morsels whose reads have been registered with the IO plane as lookahead.
    lookahead_cursor: AtomicUsize,
    completed: AtomicUsize,
    /// Whether filtered-scan lookahead refills apply; sparse random-access demand opts out.
    lookahead_enabled: bool,
    stopped: AtomicBool,
    io_bytes: AtomicU64,
    io_waits: AtomicU64,
    io_wait_nanos: AtomicU64,
    lookahead_requests: AtomicU64,
    lookahead_batches: AtomicU64,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum PlanningWaveStatus {
    Open,
    Flushing,
    Released,
}

struct PlanningWave {
    reads: Vec<IoRead>,
    completed: usize,
    status: PlanningWaveStatus,
}

impl Default for PlanningWave {
    fn default() -> Self {
        Self {
            reads: Vec::new(),
            completed: 0,
            status: PlanningWaveStatus::Open,
        }
    }
}

enum WorkerMessage {
    Run {
        scheduler: Arc<Scheduler>,
        done: mpsc::Sender<ScanStats>,
    },
    Shutdown,
}

struct Worker {
    messages: mpsc::Sender<WorkerMessage>,
    handle: Option<JoinHandle<()>>,
}

/// A set of ready morsel workers whose lifecycle is outside a timed scan.
struct MorselWorkerPool {
    workers: Vec<Worker>,
    run_lock: Mutex<()>,
}

impl MorselWorkerPool {
    fn new(threads: usize, initial_plan: Option<Arc<ExecPlan>>) -> VortexResult<Self> {
        let (ready_tx, ready_rx) = mpsc::channel();
        let mut workers = Vec::with_capacity(threads);

        for idx in 0..threads {
            let (message_tx, message_rx) = mpsc::channel();
            let ready_tx = ready_tx.clone();
            let initial_plan = initial_plan.clone();
            let handle = std::thread::Builder::new()
                .name(format!("vortex-morsel-{idx}"))
                .spawn(move || {
                    let mut arena = initial_plan.map(|plan| {
                        let arena = plan.instantiate();
                        (plan, arena)
                    });
                    if ready_tx.send(()).is_err() {
                        return;
                    }
                    while let Ok(message) = message_rx.recv() {
                        match message {
                            WorkerMessage::Run { scheduler, done } => {
                                if arena
                                    .as_ref()
                                    .is_none_or(|(plan, _)| !Arc::ptr_eq(plan, &scheduler.run.plan))
                                {
                                    let plan = Arc::clone(&scheduler.run.plan);
                                    arena = Some((Arc::clone(&plan), plan.instantiate()));
                                }
                                let stats = arena
                                    .as_mut()
                                    .map_or_else(ScanStats::default, |(_, arena)| {
                                        scheduler.worker_loop(idx, arena)
                                    });
                                drop(done.send(stats));
                            }
                            WorkerMessage::Shutdown => break,
                        }
                    }
                })
                .map_err(|err| vortex_err!("failed to spawn morsel worker: {err}"))?;
            workers.push(Worker {
                messages: message_tx,
                handle: Some(handle),
            });
        }
        drop(ready_tx);

        for _ in 0..threads {
            ready_rx
                .recv()
                .map_err(|err| vortex_err!("morsel worker failed to start: {err}"))?;
        }
        Ok(Self {
            workers,
            run_lock: Mutex::new(()),
        })
    }

    fn run(&self, scheduler: Arc<Scheduler>, threads: usize) -> VortexResult<Vec<ScanStats>> {
        let _run_guard = self.run_lock.lock();
        let (done_tx, done_rx) = mpsc::channel();
        for thread in self.workers.iter().take(threads) {
            thread
                .messages
                .send(WorkerMessage::Run {
                    scheduler: Arc::clone(&scheduler),
                    done: done_tx.clone(),
                })
                .map_err(|err| vortex_err!("failed to dispatch morsel worker: {err}"))?;
        }
        drop(done_tx);

        let mut stats = Vec::with_capacity(threads);
        for _ in 0..threads {
            stats.push(
                done_rx
                    .recv()
                    .map_err(|err| vortex_err!("morsel worker stopped early: {err}"))?,
            );
        }
        Ok(stats)
    }
}

impl Drop for MorselWorkerPool {
    fn drop(&mut self) {
        for worker in &self.workers {
            drop(worker.messages.send(WorkerMessage::Shutdown));
        }
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                drop(handle.join());
            }
        }
    }
}

impl Scheduler {
    fn new(run: Arc<WorkerRun>, workers: usize) -> Arc<Self> {
        let sort_reads_by_segment = should_sort_reads_by_segment(run.demands.as_deref());
        let lookahead_enabled = run.lookahead_morsels > 0
            && run.plan.has_filter()
            && run.io.prefers_background_reads()
            && run
                .plan
                .initial_lookahead_len(&run.morsels, run.demands.as_deref(), workers)
                > 0;
        let scheduler = Arc::new(Self {
            remaining: AtomicUsize::new(run.morsels.len()),
            run,
            workers,
            sort_reads_by_segment,
            reads: Mutex::new(HashMap::default()),
            planning_waves: Mutex::new(BTreeMap::new()),
            planning_ready: Condvar::new(),
            blocked_workers: Mutex::new(Vec::new()),
            results: Mutex::new(Vec::new()),
            error: Mutex::new(None),
            next_morsel: AtomicUsize::new(0),
            lookahead_cursor: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            lookahead_enabled,
            stopped: AtomicBool::new(false),
            io_bytes: AtomicU64::new(0),
            io_waits: AtomicU64::new(0),
            io_wait_nanos: AtomicU64::new(0),
            lookahead_requests: AtomicU64::new(0),
            lookahead_batches: AtomicU64::new(0),
        });
        if scheduler.run.morsels.is_empty() {
            scheduler.stop();
        }
        scheduler
    }

    fn submit_reads_now(self: &Arc<Self>, mut reads: Vec<IoRead>) -> u64 {
        if self.sort_reads_by_segment {
            sort_reads_by_segment(&mut reads);
        }
        let (required, speculative): (Vec<_>, Vec<_>) = reads
            .into_iter()
            .partition(|read| read.priority() == IoPriority::Required);
        self.run.io.issue_batch(&speculative, true);
        let eager_required =
            self.run.io.prefers_background_reads() || self.run.io.nowait_unsupported();
        if eager_required {
            self.run.io.issue_batch(&required, true);
        }
        u64::from(self.submit_io_batch(required)) + u64::from(self.submit_io_batch(speculative))
    }

    fn submit_planning_reads(
        self: &Arc<Self>,
        morsel: usize,
        reads: Vec<IoRead>,
        complete: bool,
        blocked: bool,
    ) -> u64 {
        let local_batches = read_batch_count(&reads);
        let wave_index = morsel / self.workers;
        let wave_start = wave_index * self.workers;
        let expected = self
            .run
            .morsels
            .len()
            .saturating_sub(wave_start)
            .min(self.workers);

        let mut waves = self.planning_waves.lock();
        let wave = waves.entry(wave_index).or_default();
        if wave.status == PlanningWaveStatus::Released {
            drop(waves);
            self.submit_reads_now(reads);
            return local_batches;
        }

        wave.reads.extend(reads);
        if complete {
            wave.completed += 1;
        }
        let should_flush = blocked || wave.completed == expected;
        if !should_flush || wave.status != PlanningWaveStatus::Open {
            return local_batches;
        }

        wave.status = PlanningWaveStatus::Flushing;
        loop {
            let pending = std::mem::take(
                &mut waves
                    .get_mut(&wave_index)
                    .vortex_expect("planning wave exists while flushing")
                    .reads,
            );
            drop(waves);
            self.submit_reads_now(pending);
            waves = self.planning_waves.lock();
            let wave = waves
                .get_mut(&wave_index)
                .vortex_expect("planning wave exists after flushing");
            if wave.reads.is_empty() {
                wave.status = PlanningWaveStatus::Released;
                self.planning_ready.notify_all();
                return local_batches;
            }
        }
    }

    fn wait_for_planning_wave(&self, morsel: usize) {
        let wave_index = morsel / self.workers;
        let mut waves = self.planning_waves.lock();
        while !self.stopped.load(Ordering::Acquire)
            && waves
                .get(&wave_index)
                .is_some_and(|wave| wave.status != PlanningWaveStatus::Released)
        {
            self.planning_ready.wait(&mut waves);
        }
    }

    fn submit_exact_lookahead(self: &Arc<Self>) {
        const BATCH_READS: usize = 64;

        if !self.run.io.prefers_background_reads() {
            return;
        }

        let priority = if self.run.plan.has_filter() {
            IoPriority::Speculative
        } else {
            IoPriority::Required
        };
        let initial_end = self.run.plan.initial_lookahead_len(
            &self.run.morsels,
            self.run.demands.as_deref(),
            self.workers,
        );
        let end = if initial_end == 0 || !self.run.plan.has_filter() {
            initial_end
        } else {
            (initial_end + self.run.lookahead_morsels).min(self.run.morsels.len())
        };
        self.lookahead_cursor.store(end, Ordering::Release);
        let mut reads = self.run.io.register_reads(
            self.run.plan.lookahead_keys(
                &self.run.morsels[..end],
                self.run.demands.as_deref().map(|demands| &demands[..end]),
            ),
            priority,
        );
        if self.sort_reads_by_segment {
            sort_reads_by_segment(&mut reads);
        }
        self.lookahead_requests
            .fetch_add(reads.len() as u64, Ordering::Relaxed);
        for batch in reads.chunks(BATCH_READS) {
            let submitted = self.submit_reads_now(batch.to_vec());
            self.lookahead_batches
                .fetch_add(submitted, Ordering::Relaxed);
        }
    }

    fn submit_io_batch(&self, reads: Vec<IoRead>) -> bool {
        if reads.is_empty() {
            return false;
        }
        let mut registered = self.reads.lock();
        for read in reads {
            registered.insert(read.key(), read);
        }
        true
    }

    fn wait(
        self: &Arc<Self>,
        worker: usize,
        morsel: usize,
        waits: &WaitSet,
        io: &IoPlane,
    ) -> VortexResult<()> {
        if waits.is_empty() {
            return Err(vortex_err!(
                "execution blocked without naming an exact dependency"
            ));
        }

        self.wait_for_planning_wave(morsel);

        let mut targets = Vec::with_capacity(waits.waits().len());
        for wait in waits.waits() {
            let Wait::Io(ticket) = wait;
            let read = self
                .run
                .io
                .read(*ticket)
                .ok_or_else(|| vortex_err!("blocked on an unknown IO ticket"))?;
            read.promote();
            targets.push(read);
        }
        if self.run.demands.is_none() || self.workers == 1 {
            for read in io.required_reads() {
                if !targets.iter().any(|target| target.key() == read.key()) {
                    targets.push(read);
                }
            }
        }
        let mut reads: Vec<_> = self.reads.lock().values().cloned().collect();
        for target in &targets {
            if !reads.iter().any(|read| read.key() == target.key()) {
                reads.push(target.clone());
            }
        }
        for read in &reads {
            self.run.io.issue(read, false);
        }

        // Dense scans benefit from harvesting scan-wide lookahead while a worker is already
        // waiting. Sparse-demand scans keep polling local to their exact/required set so random
        // access does not walk or promote unrelated morsels.
        let poll_reads = if self.run.demands.is_some() && self.workers > 1 {
            targets.clone()
        } else {
            reads
        };
        let use_poll_ownership = self.run.demands.is_none() && self.workers > 1;
        let mut owned = Vec::<IoRead>::new();
        futures::executor::block_on(poll_fn(|cx| {
            if self.stopped.load(Ordering::Acquire) {
                for read in owned.drain(..) {
                    read.release_poll(worker);
                }
                return Poll::Ready(Ok(()));
            }
            {
                let mut blocked = self.blocked_workers.lock();
                if !blocked.iter().any(|waker| waker.will_wake(cx.waker())) {
                    blocked.push(cx.waker().clone());
                }
            }
            if self.stopped.load(Ordering::Acquire) {
                for read in owned.drain(..) {
                    read.release_poll(worker);
                }
                return Poll::Ready(Ok(()));
            }

            let mut target_pending = false;
            for target in &targets {
                target_pending |= target.park(cx.waker().clone());
            }
            if !target_pending {
                for read in owned.drain(..) {
                    read.release_poll(worker);
                }
                return Poll::Ready(Ok(()));
            }

            for read in &poll_reads {
                if use_poll_ownership && !read.claim_poll(worker) {
                    continue;
                }
                if use_poll_ownership && !owned.iter().any(|owned| owned.key() == read.key()) {
                    owned.push(read.clone());
                }
                match read.poll(cx.waker()) {
                    Ok(IoReadPoll::Pending) => {
                        self.io_waits.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(IoReadPoll::Ready { bytes, wait_time }) => {
                        if use_poll_ownership {
                            read.release_poll(worker);
                            owned.retain(|owned| owned.key() != read.key());
                        }
                        self.io_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
                        self.io_wait_nanos.fetch_add(
                            u64::try_from(wait_time.as_nanos()).unwrap_or(u64::MAX),
                            Ordering::Relaxed,
                        );
                    }
                    Ok(IoReadPoll::AlreadyReady) => {
                        if use_poll_ownership {
                            read.release_poll(worker);
                            owned.retain(|owned| owned.key() != read.key());
                        }
                    }
                    Err(err) => {
                        for read in owned.drain(..) {
                            read.release_poll(worker);
                        }
                        return Poll::Ready(Err(err));
                    }
                }
            }

            if targets.iter().all(IoRead::is_ready) {
                for read in owned.drain(..) {
                    read.release_poll(worker);
                }
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }))
    }

    fn worker_loop(self: &Arc<Self>, worker: usize, arena: &mut Arena) -> ScanStats {
        let mut morsel = LocalMorsel::new(&self.run, worker, arena);
        let mut runnable = morsel.assign_next(self);

        loop {
            if self.stopped.load(Ordering::Acquire) {
                break;
            }

            if !runnable {
                break;
            }
            match morsel.run(self) {
                Ok(LocalPoll::Runnable) => {}
                Ok(LocalPoll::Blocked(waits)) => {
                    let wait_start = self.run.observe_timing.then(Instant::now);
                    let result = self.wait(worker, morsel.index, &waits, &morsel.io);
                    if let Some(wait_start) = wait_start {
                        morsel.stats.worker_io_wait_time += wait_start.elapsed();
                    }
                    if let Err(err) = result {
                        self.fail(err);
                        break;
                    }
                }
                Ok(LocalPoll::Complete { index, batch }) => {
                    self.complete(index, batch);
                    runnable = !self.stopped.load(Ordering::Acquire) && morsel.assign_next(self);
                }
                Err(err) => {
                    self.fail(err);
                    break;
                }
            }
        }
        morsel.stats
    }

    fn complete(self: &Arc<Self>, index: usize, batch: Option<ArrayRef>) {
        match &self.run.completion {
            Some(sink) => sink(index, batch),
            None => {
                if let Some(batch) = batch {
                    self.results.lock().push((index, batch));
                }
            }
        }
        let completed = self.completed.fetch_add(1, Ordering::AcqRel) + 1;
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.stop();
            return;
        }
        self.refill_lookahead(completed);
    }

    /// Keep the lookahead window ahead of the active workers on filtered scans so background
    /// reads of later morsels overlap the current morsels' execution and coalesce together.
    fn refill_lookahead(self: &Arc<Self>, completed: usize) {
        if !self.lookahead_enabled {
            return;
        }
        let target = (completed + self.workers.saturating_mul(2) + self.run.lookahead_morsels)
            .min(self.run.morsels.len());
        let start = self.lookahead_cursor.load(Ordering::Acquire);
        if target <= start
            || self
                .lookahead_cursor
                .compare_exchange(start, target, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return;
        }
        self.submit_lookahead_window(start, target);
    }

    fn submit_lookahead_window(self: &Arc<Self>, start: usize, end: usize) {
        const BATCH_READS: usize = 64;
        if start >= end {
            return;
        }
        let priority = if self.run.plan.has_filter() {
            IoPriority::Speculative
        } else {
            IoPriority::Required
        };
        let keys = self.run.plan.lookahead_keys(
            &self.run.morsels[start..end],
            self.run
                .demands
                .as_deref()
                .map(|demands| &demands[start..end]),
        );
        let mut reads = self.run.io.register_reads(keys, priority);
        if self.sort_reads_by_segment {
            sort_reads_by_segment(&mut reads);
        }
        self.lookahead_requests
            .fetch_add(reads.len() as u64, Ordering::Relaxed);
        for batch in reads.chunks(BATCH_READS) {
            let submitted = self.submit_reads_now(batch.to_vec());
            self.lookahead_batches
                .fetch_add(submitted, Ordering::Relaxed);
        }
    }

    fn fail(&self, err: VortexError) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            *self.error.lock() = Some(err);
            self.wake_blocked_workers();
        }
    }

    fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            self.wake_blocked_workers();
        }
    }

    fn wake_blocked_workers(&self) {
        self.planning_ready.notify_all();
        for waker in std::mem::take(&mut *self.blocked_workers.lock()) {
            waker.wake();
        }
    }

    fn finish(&self, worker_stats: Vec<ScanStats>) -> VortexResult<(Vec<ArrayRef>, ScanStats)> {
        if let Some(err) = self.error.lock().take() {
            return Err(err);
        }

        let mut stats = ScanStats::default();
        for worker in worker_stats {
            stats.merge(&worker);
        }
        stats.io_bytes += self.io_bytes.load(Ordering::Relaxed);
        stats.io_requests += self.lookahead_requests.load(Ordering::Relaxed);
        stats.io_batches += self.lookahead_batches.load(Ordering::Relaxed);
        stats.io_waits = self.io_waits.load(Ordering::Relaxed);
        stats.io_wait_time = Duration::from_nanos(self.io_wait_nanos.load(Ordering::Relaxed));

        let mut results = std::mem::take(&mut *self.results.lock());
        results.sort_unstable_by_key(|(index, _)| *index);
        Ok((results.into_iter().map(|(_, array)| array).collect(), stats))
    }
}

fn should_sort_reads_by_segment(demands: Option<&[Mask]>) -> bool {
    !demands.is_some_and(|demands| demands.iter().all(Mask::all_true))
}

fn sort_reads_by_segment(reads: &mut [IoRead]) {
    reads.sort_unstable_by_key(|read| match read.key() {
        IoKey::Segment(id) => *id,
    });
}

fn read_batch_count(reads: &[IoRead]) -> u64 {
    u64::from(
        reads
            .iter()
            .any(|read| read.priority() == IoPriority::Required),
    ) + u64::from(
        reads
            .iter()
            .any(|read| read.priority() == IoPriority::Speculative),
    )
}

enum LocalPoll {
    Runnable,
    Blocked(WaitSet),
    Complete {
        index: usize,
        batch: Option<ArrayRef>,
    },
}

impl<'a> LocalMorsel<'a> {
    fn new(run: &WorkerRun, worker: usize, arena: &'a mut Arena) -> Self {
        Self {
            arena,
            worker,
            io: IoPlane::new(Arc::clone(&run.io)),
            phase: TaskPhase::Plan,
            index: 0,
            range: 0..0,
            demand: Mask::new_true(0),
            active: false,
            morsel_started: None,
            morsel_io_uses_start: 0,
            morsel_io_requests_start: 0,
            morsel_io_batches_start: 0,
            morsel_io_blocks_start: 0,
            stats: ScanStats::default(),
        }
    }

    fn assign_next(&mut self, scheduler: &Scheduler) -> bool {
        let index = scheduler.next_morsel.fetch_add(1, Ordering::Relaxed);
        let Some(range) = scheduler.run.morsels.get(index).cloned() else {
            self.active = false;
            return false;
        };

        self.index = index;
        self.range = range.clone();
        self.demand = scheduler.run.demands.as_ref().map_or_else(
            || {
                let len = usize::try_from(range.end - range.start)
                    .unwrap_or_else(|_| vortex_panic!("morsel row count exceeds usize"));
                Mask::new_true(len)
            },
            |demands| demands[index].clone(),
        );
        self.phase = TaskPhase::Plan;
        self.active = true;
        self.morsel_started = scheduler.run.observe_morsels.then(Instant::now);
        self.morsel_io_uses_start = self.stats.io_uses;
        self.morsel_io_requests_start = self.stats.io_requests;
        self.morsel_io_batches_start = self.stats.io_batches;
        self.morsel_io_blocks_start = self.stats.execute_io_blocks;
        if scheduler.run.observe_morsels {
            self.stats.begin_morsel_trace(
                index,
                self.worker,
                range.start,
                range.end,
                self.demand.true_count() as u64,
            );
        }
        self.io.clear();
        begin_morsel(self.arena, scheduler.run.plan.root(), range);
        true
    }

    fn run(&mut self, scheduler: &Arc<Scheduler>) -> VortexResult<LocalPoll> {
        debug_assert!(self.active);
        match self.phase {
            TaskPhase::Plan => {
                self.stats.plan_polls += 1;
                let phase_start = scheduler.run.observe_timing.then(Instant::now);
                let poll = poll_plan_morsel(
                    self.arena,
                    scheduler.run.plan.root(),
                    &self.demand,
                    &self.io,
                    ScanCaches::new(&scheduler.run.cells, &scheduler.run.dictionary_values),
                    &mut self.stats,
                )?;
                if let Some(phase_start) = phase_start {
                    self.stats.planning_time += phase_start.elapsed();
                }
                self.stats.io_batches += scheduler.submit_planning_reads(
                    self.index,
                    self.io.take_reads(),
                    matches!(poll, PlanPoll::Complete),
                    matches!(poll, PlanPoll::Blocked(_)),
                );
                match poll {
                    PlanPoll::Item(_) => Ok(LocalPoll::Runnable),
                    PlanPoll::Blocked(waits) => Ok(LocalPoll::Blocked(waits)),
                    PlanPoll::Complete => {
                        self.stats.morsels += 1;
                        self.stats.morsel_rows += self.demand.len() as u64;
                        self.stats.selected_rows += self.demand.true_count() as u64;
                        self.phase = TaskPhase::Execute;
                        Ok(LocalPoll::Runnable)
                    }
                }
            }
            TaskPhase::Execute => {
                self.stats.execute_polls += 1;
                let phase_start = scheduler.run.observe_timing.then(Instant::now);
                let poll = poll_execute_morsel(
                    self.arena,
                    scheduler.run.plan.root(),
                    &self.demand,
                    &self.io,
                    ScanCaches::new(&scheduler.run.cells, &scheduler.run.dictionary_values),
                    &scheduler.run.session,
                    &mut self.stats,
                )?;
                if let Some(phase_start) = phase_start {
                    self.stats.execution_time += phase_start.elapsed();
                }
                match poll {
                    ExecPoll::Value(batch) => {
                        let array = batch.value.into_array()?;
                        let array = (!array.is_empty()).then_some(array);
                        self.finish_morsel(scheduler, array)
                    }
                    ExecPoll::Yield(_) => Ok(LocalPoll::Runnable),
                    ExecPoll::Blocked(waits) => {
                        self.stats.execute_io_blocks += 1;
                        Ok(LocalPoll::Blocked(waits))
                    }
                    ExecPoll::Done => self.finish_morsel(scheduler, None),
                }
            }
        }
    }

    fn finish_morsel(
        &mut self,
        scheduler: &Scheduler,
        batch: Option<ArrayRef>,
    ) -> VortexResult<LocalPoll> {
        let retire_start = scheduler.run.observe_timing.then(Instant::now);
        retire_morsel(
            self.arena,
            scheduler.run.plan.root(),
            &scheduler.run.cells,
            &mut self.stats,
        );
        if let Some(retire_start) = retire_start {
            self.stats.retire_time += retire_start.elapsed();
        }
        if scheduler.run.observe_morsels {
            let output_rows = batch.as_ref().map_or(0, |array| array.len() as u64);
            let segment_ids = self.io.segment_ids();
            let wall_time = self
                .morsel_started
                .take()
                .map_or_else(Duration::default, |started| started.elapsed());
            self.stats
                .finish_morsel_trace(self.index, output_rows, segment_ids, wall_time);
        }
        self.io.clear();
        if batch.is_some() && self.stats.time_to_first_batch.is_none() {
            self.stats.time_to_first_batch = Some(scheduler.run.start.elapsed());
        }
        let io_uses = self.stats.io_uses - self.morsel_io_uses_start;
        let io_requests = self.stats.io_requests - self.morsel_io_requests_start;
        let io_batches = self.stats.io_batches - self.morsel_io_batches_start;
        let io_blocks = self.stats.execute_io_blocks - self.morsel_io_blocks_start;
        self.stats
            .record_morsel_io(io_uses, io_requests, io_batches, io_blocks);
        self.active = false;
        Ok(LocalPoll::Complete {
            index: self.index,
            batch,
        })
    }
}

impl MorselScan {
    /// Configure a scan over a built plan.
    pub fn new(
        plan: Arc<ExecPlan>,
        segments: Arc<dyn SegmentSource>,
        session: VortexSession,
    ) -> Self {
        let morsels = Arc::from(morsels(&plan, 0));
        Self {
            plan,
            segments,
            session,
            morsels,
            demands: None,
            threads: 1,
            share_decodes: true,
            observe: false,
            lookahead_morsels: 0,
            completion: None,
        }
    }

    /// Keep this many morsels beyond the active window visible to background I/O on filtered
    /// scans, refilled as morsels retire. Unfiltered scans already register the whole plan.
    pub fn with_lookahead_morsels(mut self, morsels: usize) -> Self {
        self.lookahead_morsels = morsels;
        self
    }

    /// Deliver each morsel's output through `sink` as it completes instead of collecting it.
    pub fn with_completion_sink(
        mut self,
        sink: impl Fn(usize, Option<ArrayRef>) + Send + Sync + 'static,
    ) -> Self {
        self.completion = Some(Arc::new(sink));
        self
    }

    /// Set the number of driving threads and affinity-owned active morsels.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads.max(1);
        self
    }

    /// Override the morsel cut.
    pub fn with_morsels(mut self, morsels: Vec<Range<u64>>) -> Self {
        self.morsels = Arc::from(morsels);
        self.demands = None;
        self
    }

    /// Override the morsel cut and initial demand for each range.
    ///
    /// This is the sparse-selection entry point: nodes still plan over a dense range, but execute
    /// only rows selected by its same-length demand mask.
    pub fn with_morsel_demands(mut self, morsels: Vec<(Range<u64>, Mask)>) -> VortexResult<Self> {
        let mut ranges = Vec::with_capacity(morsels.len());
        let mut demands = Vec::with_capacity(morsels.len());
        let mut previous_end = 0;
        for (range, demand) in morsels {
            let rows = range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| vortex_err!("morsel range is reversed: {range:?}"))?;
            let rows =
                usize::try_from(rows).map_err(|_| vortex_err!("morsel row count exceeds usize"))?;
            if demand.len() != rows {
                return Err(vortex_err!(
                    "morsel demand length {} does not match range {range:?} length {rows}",
                    demand.len()
                ));
            }
            if range.end > self.plan.row_count() {
                return Err(vortex_err!(
                    "morsel range {range:?} exceeds plan row count {}",
                    self.plan.row_count()
                ));
            }
            if !ranges.is_empty() && range.start < previous_end {
                return Err(vortex_err!(
                    "morsel ranges must be sorted and non-overlapping"
                ));
            }
            previous_end = range.end;
            ranges.push(range);
            demands.push(demand);
        }
        self.morsels = Arc::from(ranges);
        self.demands = Some(Arc::from(demands));
        Ok(self)
    }

    /// Enable or disable the leased shared decoded cells.
    pub fn with_share_decodes(mut self, share: bool) -> Self {
        self.share_decodes = share;
        self
    }

    /// Enable opt-in scan and per-morsel tracing with phase timings and work counters.
    pub fn with_observability(mut self, observe: bool) -> Self {
        self.observe = observe;
        self
    }

    fn lease_counts(&self) -> HashMap<IoKey, usize> {
        let mut counts: HashMap<IoKey, usize> = HashMap::default();
        for (key, range) in self.plan.flat_uses() {
            let overlapping = demanding_morsels(&self.morsels, self.demands.as_deref(), &range);
            if overlapping > 0 {
                *counts.entry(key).or_default() += overlapping;
            }
        }
        counts
    }

    /// The morsels this scan will drive.
    pub fn morsel_ranges(&self) -> &[Range<u64>] {
        &self.morsels
    }

    /// Run the scan, returning batches in row order plus the run's counters.
    pub fn run(&self) -> VortexResult<(Vec<ArrayRef>, ScanStats)> {
        MorselExecutor::new(Arc::clone(&self.plan), self.threads)?.run(self)
    }

    /// Run the scan with worker creation and shutdown outside the measured interval.
    #[cfg(any(test, feature = "_test-harness"))]
    pub(crate) fn run_timed(&self) -> VortexResult<(Vec<ArrayRef>, ScanStats, Duration)> {
        MorselExecutor::new(Arc::clone(&self.plan), self.threads)?.run_timed(self)
    }
}

impl MorselExecutor {
    /// Create a reusable executor with initialized per-worker arenas.
    pub fn new(plan: Arc<ExecPlan>, threads: usize) -> VortexResult<Self> {
        let threads = threads.max(1);
        let workers = if threads == 1 {
            ExecutorWorkers::Inline(Mutex::new(plan.instantiate()))
        } else {
            ExecutorWorkers::Pool(Arc::new(MorselWorkerPool::new(
                threads,
                Some(Arc::clone(&plan)),
            )?))
        };
        Ok(Self {
            plan,
            workers,
            threads,
        })
    }

    /// Create an executor backed by a process-wide pool sized to detected hardware parallelism.
    ///
    /// There is deliberately no fixed worker cap. Requests within the machine's detected
    /// parallelism reuse persistent workers; larger explicitly configured requests receive a
    /// dedicated reusable pool instead. This keeps reopen-heavy workloads from creating OS
    /// threads inside every scan without limiting the executor's supported concurrency.
    pub fn shared(plan: Arc<ExecPlan>, threads: usize) -> VortexResult<Self> {
        static POOL: OnceLock<Result<(Arc<MorselWorkerPool>, usize), String>> = OnceLock::new();

        let threads = threads.max(1);
        let workers = if threads == 1 {
            ExecutorWorkers::Inline(Mutex::new(plan.instantiate()))
        } else {
            let (pool, shared_threads) = POOL
                .get_or_init(|| {
                    let shared_threads = get_available_parallelism().unwrap_or(1).max(1);
                    MorselWorkerPool::new(shared_threads, None)
                        .map(|pool| (Arc::new(pool), shared_threads))
                        .map_err(|err| err.to_string())
                })
                .as_ref()
                .map_err(|err| vortex_err!("failed to initialize shared morsel workers: {err}"))?;
            if threads > *shared_threads {
                return Self::new(plan, threads);
            }
            ExecutorWorkers::Pool(Arc::clone(pool))
        };
        Ok(Self {
            plan,
            workers,
            threads,
        })
    }

    /// The number of affinity-owned workers in this executor.
    pub fn threads(&self) -> usize {
        self.threads
    }

    /// Run a scan using this executor's initialized workers.
    pub fn run(&self, scan: &MorselScan) -> VortexResult<(Vec<ArrayRef>, ScanStats)> {
        let (batches, stats, _) = self.run_timed(scan)?;
        Ok((batches, stats))
    }

    #[allow(clippy::cognitive_complexity)]
    fn run_timed(&self, scan: &MorselScan) -> VortexResult<(Vec<ArrayRef>, ScanStats, Duration)> {
        if !Arc::ptr_eq(&self.plan, &scan.plan) {
            return Err(vortex_err!(
                "morsel executor and scan must share the same execution plan"
            ));
        }
        if !self.plan.supports_ranges(&scan.morsels) {
            return Err(vortex_err!(
                "morsel scan reaches row ranges that were not materialized in the execution plan"
            ));
        }
        let threads = self.threads();
        let start = Instant::now();
        let observe_summary =
            scan.observe && tracing::enabled!(target: "vortex_morsel::scan", tracing::Level::INFO);
        let observe_morsels = scan.observe
            && tracing::enabled!(target: "vortex_morsel::morsel", tracing::Level::DEBUG);
        let observe_timing = observe_summary || observe_morsels;
        let span = observe_summary.then(|| {
            tracing::info_span!(
                target: "vortex_morsel::scan",
                "morsel_scan",
                threads,
                plan_nodes = self.plan.len(),
                flat_nodes = self.plan.flat_uses().count(),
                morsels = scan.morsels.len(),
                sparse_demand = scan.demands.is_some(),
                share_decodes = scan.share_decodes,
            )
        });
        let _span_guard = span.as_ref().map(tracing::Span::enter);
        let cells = if scan.share_decodes {
            SharedCells::with_leases(scan.lease_counts())
        } else {
            SharedCells::disabled()
        };
        let run = Arc::new(WorkerRun {
            plan: Arc::clone(&scan.plan),
            session: scan.session.clone(),
            morsels: Arc::clone(&scan.morsels),
            demands: scan.demands.clone(),
            io: IoService::new(Arc::clone(&scan.segments)),
            cells,
            dictionary_values: (0..scan.plan.len()).map(|_| OnceLock::new()).collect(),
            start,
            observe_timing,
            observe_morsels,
            lookahead_morsels: scan.lookahead_morsels,
            completion: scan.completion.clone(),
        });

        let scheduler = Scheduler::new(Arc::clone(&run), threads);
        scheduler.submit_exact_lookahead();
        let setup_time = if observe_timing {
            start.elapsed()
        } else {
            Duration::default()
        };
        let worker_stats = match &self.workers {
            ExecutorWorkers::Inline(arena) => {
                vec![scheduler.worker_loop(0, &mut arena.lock())]
            }
            ExecutorWorkers::Pool(workers) => workers.run(Arc::clone(&scheduler), threads)?,
        };
        let (batches, mut stats) = scheduler.finish(worker_stats)?;
        stats
            .morsel_traces
            .sort_unstable_by_key(|trace| trace.index);

        debug_assert_eq!(
            run.cells.live(),
            0,
            "every lease must be released by the end of the scan"
        );

        let wall = start.elapsed();
        if observe_timing {
            stats.setup_time = setup_time;
            stats.wall_time = wall;
        }
        if observe_morsels {
            for trace in &stats.morsel_traces {
                let span_rows = trace.row_end - trace.row_start;
                let density_ppm = trace
                    .selected_rows
                    .saturating_mul(1_000_000)
                    .checked_div(span_rows)
                    .unwrap_or_default();
                tracing::debug!(
                    target: "vortex_morsel::morsel",
                    index = trace.index,
                    worker = trace.worker,
                    row_start = trace.row_start,
                    row_end = trace.row_end,
                    span_rows,
                    selected_rows = trace.selected_rows,
                    selection_density_ppm = density_ppm,
                    output_rows = trace.output_rows,
                    plan_polls = trace.plan_polls,
                    execute_polls = trace.execute_polls,
                    planning_us = u64::try_from(trace.planning_time.as_micros()).unwrap_or(u64::MAX),
                    execution_us = u64::try_from(trace.execution_time.as_micros()).unwrap_or(u64::MAX),
                    worker_io_wait_us = u64::try_from(trace.worker_io_wait_time.as_micros()).unwrap_or(u64::MAX),
                    retire_us = u64::try_from(trace.retire_time.as_micros()).unwrap_or(u64::MAX),
                    wall_us = u64::try_from(trace.wall_time.as_micros()).unwrap_or(u64::MAX),
                    io_uses = trace.io_uses,
                    io_requests_created = trace.io_requests,
                    io_batches = trace.io_batches,
                    io_cell_hits = trace.io_cell_hits,
                    io_registered = trace.io_registered,
                    nowait_attempts = trace.nowait_attempts,
                    nowait_hits = trace.nowait_hits,
                    nowait_misses = trace.nowait_misses,
                    nowait_unsupported = trace.nowait_unsupported,
                    execute_io_blocks = trace.execute_io_blocks,
                    segments = ?trace.segment_ids,
                    decoded_segments = ?trace.decoded_segment_ids,
                    reused_segments = ?trace.reused_segment_ids,
                    "morsel complete"
                );
            }
        }
        if observe_summary {
            let density_ppm = stats
                .selected_rows
                .saturating_mul(1_000_000)
                .checked_div(stats.morsel_rows)
                .unwrap_or_default();
            tracing::info!(
                target: "vortex_morsel::scan",
                wall_us = u64::try_from(wall.as_micros()).unwrap_or(u64::MAX),
                setup_us = u64::try_from(stats.setup_time.as_micros()).unwrap_or(u64::MAX),
                planning_us_sum = u64::try_from(stats.planning_time.as_micros()).unwrap_or(u64::MAX),
                execution_us_sum = u64::try_from(stats.execution_time.as_micros()).unwrap_or(u64::MAX),
                worker_io_wait_us_sum = u64::try_from(stats.worker_io_wait_time.as_micros()).unwrap_or(u64::MAX),
                retire_us_sum = u64::try_from(stats.retire_time.as_micros()).unwrap_or(u64::MAX),
                plan_polls = stats.plan_polls,
                execute_polls = stats.execute_polls,
                morsel_rows = stats.morsel_rows,
                selected_rows = stats.selected_rows,
                selection_density_ppm = density_ppm,
                io_uses = stats.io_uses,
                io_requests = stats.io_requests,
                io_batches = stats.io_batches,
                io_bytes = stats.io_bytes,
                io_waits = stats.io_waits,
                io_wait_us_sum = u64::try_from(stats.io_wait_time.as_micros()).unwrap_or(u64::MAX),
                execute_io_blocks = stats.execute_io_blocks,
                blocked_morsels = stats.morsels_blocked_for_io,
                io_uses_per_morsel_min = stats.io_uses_per_morsel_min.unwrap_or_default(),
                io_uses_per_morsel_max = stats.io_uses_per_morsel_max,
                io_blocks_per_morsel_max = stats.io_blocks_per_morsel_max,
                decodes = stats.decodes,
                decode_reuses = stats.decode_reuses,
                output_batches = batches.len(),
                "morsel scan complete"
            );
        }
        Ok((batches, stats, wall))
    }
}

#[cfg(test)]
mod tests {
    use vortex_mask::Mask;

    use super::demanding_morsels;
    use super::overlapping_morsels;
    use super::should_sort_reads_by_segment;

    #[test]
    fn counts_overlapping_sorted_morsels() {
        let morsels = [0..10, 10..20, 20..30];

        assert_eq!(overlapping_morsels(&morsels, &(10..20)), 1);
        assert_eq!(overlapping_morsels(&morsels, &(5..25)), 3);
        assert_eq!(overlapping_morsels(&morsels, &(0..30)), 3);
        assert_eq!(overlapping_morsels(&morsels, &(30..40)), 0);
        assert_eq!(overlapping_morsels(&morsels, &(31..40)), 0);
    }

    #[test]
    fn counts_only_morsels_with_demand_in_range() {
        let morsels = [0..10, 10..20];
        let demands = [Mask::from_indices(10, [1, 8]), Mask::from_indices(10, [2])];

        assert_eq!(demanding_morsels(&morsels, Some(&demands), &(0..5)), 1);
        assert_eq!(demanding_morsels(&morsels, Some(&demands), &(3..8)), 0);
        assert_eq!(demanding_morsels(&morsels, Some(&demands), &(8..13)), 2);
        assert_eq!(demanding_morsels(&morsels, Some(&demands), &(13..20)), 0);
    }

    #[test]
    fn exact_demands_preserve_plan_order() {
        let exact = [Mask::new_true(4), Mask::new_true(2)];
        assert!(!should_sort_reads_by_segment(Some(&exact)));
    }

    #[test]
    fn sparse_or_dense_scans_sort_by_segment() {
        let sparse = [Mask::from_indices(4, [1])];
        assert!(should_sort_reads_by_segment(Some(&sparse)));
        assert!(should_sort_reads_by_segment(None));
    }
}
