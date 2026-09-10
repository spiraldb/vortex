// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The scheduler-visible IO plane.
//!
//! Nodes *name* reads during planning: [`PlanCx::register`](crate::PlanCx::register) takes an
//! [`IoBatch`] of [`IoUse`]s, each keyed to a whole stored unit, and hands back an [`IoTicket`].
//! Execution may resolve an unissued required ticket through a caller-provided non-blocking
//! probe; otherwise it can only clone an already-ready cell or suspend on that exact ticket.
//!
//! A scan owns one [`IoService`] but never touches storage. Reads the scheduler wants started
//! leave the scan as [`IoDemand`] items on a stream, and whoever owns storage answers each one
//! through [`IoCompletions`]. Each affinity-owned morsel has a small [`IoPlane`] that records only
//! the tickets named by that morsel; the service deduplicates raw reads scan-wide, and a blocked
//! worker parks on its exact cells until they are completed.

use std::cell::RefCell;
use std::ops::Range;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use futures::channel::mpsc;
use parking_lot::Mutex;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_layout::segments::ReadAtNowait;
use vortex_layout::segments::SegmentId;
use vortex_utils::aliases::hash_map::HashMap;
use vortex_utils::aliases::hash_set::HashSet;

use crate::stats::ScanStats;

/// The scan-wide key of one whole stored unit.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IoKey {
    /// A layout segment.
    Segment(SegmentId),
}

/// A ticket handed back by registration, naming the cell the read will land in.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct IoTicket(IoKey);

impl IoTicket {
    /// The cell this ticket names.
    pub fn key(&self) -> IoKey {
        self.0
    }
}

/// Scheduler priority attached by the parent operator while planning a read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoPriority {
    /// Needed to start the next execution phase.
    Required,
    /// Useful lookahead that may finish while required CPU work runs.
    Speculative,
}

/// Identifies the node that emitted a use, so the scheduler can attribute and cancel it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProducerId(pub u32);

/// One named read.
#[derive(Clone, Debug)]
pub struct IoUse {
    /// The whole stored unit this use covers.
    pub key: IoKey,
    /// The rows of the stored unit, frozen at emission.
    pub extent: Range<u64>,
    /// The inverse image of `extent` in root coordinates, stamped at emission. The scheduler
    /// reads demand verdicts over this range without ever seeing an offset map.
    pub source_range: Range<u64>,
    /// The node that emitted this use.
    pub producer: ProducerId,
    /// The estimated size of the read, for admission accounting.
    pub estimated_bytes: usize,
}

/// A batch of uses emitted by one planning step.
#[derive(Clone, Debug, Default)]
pub struct IoBatch {
    uses: Vec<IoUse>,
}

impl IoBatch {
    /// An empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a use to the batch.
    pub fn push(&mut self, r#use: IoUse) {
        self.uses.push(r#use);
    }

    /// The uses in this batch.
    pub fn uses(&self) -> &[IoUse] {
        &self.uses
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.uses.is_empty()
    }
}

impl FromIterator<IoUse> for IoBatch {
    fn from_iter<T: IntoIterator<Item = IoUse>>(iter: T) -> Self {
        Self {
            uses: iter.into_iter().collect(),
        }
    }
}

/// One read the scan wants performed, handed out of plan execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoRequest {
    /// The stored unit to read.
    pub key: IoKey,
    /// Whether execution is known to need this read before it can continue.
    pub priority: IoPriority,
}

/// What a scan asks of the caller that owns storage.
#[derive(Debug)]
pub enum IoDemand {
    /// Start these reads. One item is one scheduling batch: a storage layer that coalesces
    /// adjacent ranges should register the whole batch before making any member eligible.
    Start(Vec<IoRequest>),
    /// Execution is blocked on this read; finish it ahead of speculative work.
    Promote(IoKey),
}

/// The stream of [`IoDemand`] a scan emits. It ends when the scan is dropped.
pub type IoDemandStream = mpsc::UnboundedReceiver<IoDemand>;

/// A caller-provided probe that resolves a read without waiting on storage.
pub type NowaitProbe = Arc<dyn Fn(IoKey) -> VortexResult<ReadAtNowait> + Send + Sync>;

/// Answers the reads a scan handed out through its [`IoDemandStream`].
///
/// Completions arriving after the scan has been dropped are ignored.
#[derive(Clone)]
pub struct IoCompletions {
    service: Weak<IoService>,
}

impl IoCompletions {
    /// Deliver the bytes (or the failure) for one read.
    ///
    /// Returns `false` once the scan is gone, so a driver can stop serving it.
    pub fn complete(&self, key: IoKey, result: VortexResult<BufferHandle>) -> bool {
        match self.service.upgrade() {
            Some(service) => {
                service.complete(key, result);
                true
            }
            None => false,
        }
    }

    /// Whether the scan these completions belong to has been dropped.
    pub fn is_closed(&self) -> bool {
        self.service.strong_count() == 0
    }
}

enum CellState {
    Unissued,
    Requested { started: Instant },
    Ready(BufferHandle),
    Failed(Arc<str>),
}

struct IoCell {
    key: IoKey,
    state: Mutex<CellState>,
    waiters: Mutex<Vec<Waker>>,
    required: AtomicBool,
    submitted: AtomicBool,
}

#[derive(Clone, Copy)]
enum FirstNeedState {
    Ready,
    Requested,
    Unissued,
}

#[derive(Default)]
struct IoOracleTrace {
    started: Vec<IoKey>,
    started_batches: Vec<Vec<IoKey>>,
    completed: Vec<IoKey>,
    completed_at: HashMap<IoKey, Instant>,
    first_needed: Vec<IoKey>,
    first_needed_at: HashMap<IoKey, Instant>,
    seen_needed: HashSet<IoKey>,
    first_need_ready: u64,
    first_need_requested: u64,
    first_need_unissued: u64,
}

struct IoOracle {
    replay_rank: HashMap<IoKey, usize>,
    trace: Mutex<IoOracleTrace>,
    reordered_reads: AtomicU64,
    reordered_batches: AtomicU64,
}

/// A benchmark-only hindsight view of one scan's I/O schedule.
#[derive(Clone, Debug, Default)]
pub(crate) struct IoOracleSnapshot {
    pub(crate) learned_order: Vec<IoKey>,
    pub(crate) replay_keys: u64,
    pub(crate) replay_hits: u64,
    pub(crate) replay_misses: u64,
    pub(crate) reordered_reads: u64,
    pub(crate) reordered_batches: u64,
    pub(crate) start_inversions: u64,
    pub(crate) start_pairs: u64,
    pub(crate) completion_inversions: u64,
    pub(crate) completion_pairs: u64,
    pub(crate) unused_started: u64,
    pub(crate) first_need_ready: u64,
    pub(crate) first_need_requested: u64,
    pub(crate) first_need_unissued: u64,
    pub(crate) late_read_time: Duration,
    pub(crate) late_read_time_max: Duration,
    pub(crate) ready_lead_time: Duration,
}

impl IoOracleSnapshot {
    pub(crate) fn apply_to(&self, stats: &mut ScanStats) {
        stats.io_oracle_learned_keys = u64::try_from(self.learned_order.len()).unwrap_or(u64::MAX);
        stats.io_oracle_replay_keys = self.replay_keys;
        stats.io_oracle_replay_hits = self.replay_hits;
        stats.io_oracle_replay_misses = self.replay_misses;
        stats.io_oracle_reordered_reads = self.reordered_reads;
        stats.io_oracle_reordered_batches = self.reordered_batches;
        stats.io_oracle_start_inversions = self.start_inversions;
        stats.io_oracle_start_pairs = self.start_pairs;
        stats.io_oracle_completion_inversions = self.completion_inversions;
        stats.io_oracle_completion_pairs = self.completion_pairs;
        stats.io_oracle_unused_started = self.unused_started;
        stats.io_oracle_first_need_ready = self.first_need_ready;
        stats.io_oracle_first_need_requested = self.first_need_requested;
        stats.io_oracle_first_need_unissued = self.first_need_unissued;
        stats.io_oracle_late_read_time = self.late_read_time;
        stats.io_oracle_late_read_time_max = self.late_read_time_max;
        stats.io_oracle_ready_lead_time = self.ready_lead_time;
    }
}

impl IoOracle {
    #[cfg(any(test, feature = "_test-harness"))]
    fn new(replay: impl IntoIterator<Item = IoKey>) -> Self {
        let mut replay_rank = HashMap::default();
        for key in replay {
            let next = replay_rank.len();
            replay_rank.entry(key).or_insert(next);
        }
        Self {
            replay_rank,
            trace: Mutex::new(IoOracleTrace::default()),
            reordered_reads: AtomicU64::new(0),
            reordered_batches: AtomicU64::new(0),
        }
    }

    fn observe_start(&self, requests: &[IoRequest]) {
        let mut trace = self.trace.lock();
        let batch = requests
            .iter()
            .map(|request| request.key)
            .collect::<Vec<_>>();
        trace.started.extend(batch.iter().copied());
        trace.started_batches.push(batch);
    }

    fn observe_complete(&self, key: IoKey) {
        let mut trace = self.trace.lock();
        trace.completed.push(key);
        trace.completed_at.entry(key).or_insert_with(Instant::now);
    }

    fn observe_need(&self, key: IoKey, state: FirstNeedState) {
        let mut trace = self.trace.lock();
        if !trace.seen_needed.insert(key) {
            return;
        }
        trace.first_needed.push(key);
        trace.first_needed_at.insert(key, Instant::now());
        match state {
            FirstNeedState::Ready => {
                trace.first_need_ready += 1;
            }
            FirstNeedState::Requested => {
                trace.first_need_requested += 1;
            }
            FirstNeedState::Unissued => {
                trace.first_need_unissued += 1;
            }
        }
    }

    fn sort_reads(&self, reads: &mut [IoRead]) {
        if self.replay_rank.is_empty() {
            sort_reads_by_key(reads);
            return;
        }
        let before = reads.iter().map(IoRead::key).collect::<Vec<_>>();
        reads.sort_unstable_by(|left, right| {
            match (
                self.replay_rank.get(&left.key()),
                self.replay_rank.get(&right.key()),
            ) {
                (Some(left), Some(right)) => left.cmp(right),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => io_key_ordinal(left.key()).cmp(&io_key_ordinal(right.key())),
            }
        });
        let changed = before
            .iter()
            .zip(reads.iter())
            .filter(|(before, after)| **before != after.key())
            .count();
        if changed > 0 {
            self.reordered_reads.fetch_add(
                u64::try_from(changed).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            self.reordered_batches.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> IoOracleSnapshot {
        let trace = self.trace.lock();
        let need_rank = trace
            .first_needed
            .iter()
            .copied()
            .enumerate()
            .map(|(rank, key)| (key, rank))
            .collect::<HashMap<_, _>>();
        let (start_inversions, start_pairs) =
            batch_order_inversions(&trace.started_batches, &need_rank);
        let (completion_inversions, completion_pairs) =
            completion_batch_inversions(&trace.completed, &trace.started_batches, &need_rank);
        let replay_hits = trace
            .started
            .iter()
            .filter(|key| self.replay_rank.contains_key(*key))
            .count();
        let unused_started = trace
            .started
            .iter()
            .filter(|key| !need_rank.contains_key(*key))
            .count();
        let mut late_read_time = Duration::ZERO;
        let mut late_read_time_max = Duration::ZERO;
        let mut ready_lead_time = Duration::ZERO;
        for key in &trace.first_needed {
            let (Some(needed), Some(completed)) =
                (trace.first_needed_at.get(key), trace.completed_at.get(key))
            else {
                continue;
            };
            if completed > needed {
                let late = completed.duration_since(*needed);
                late_read_time += late;
                late_read_time_max = late_read_time_max.max(late);
            } else {
                ready_lead_time += needed.duration_since(*completed);
            }
        }
        IoOracleSnapshot {
            learned_order: deadline_miss_order(&trace),
            replay_keys: u64::try_from(self.replay_rank.len()).unwrap_or(u64::MAX),
            replay_hits: u64::try_from(replay_hits).unwrap_or(u64::MAX),
            replay_misses: u64::try_from(trace.started.len().saturating_sub(replay_hits))
                .unwrap_or(u64::MAX),
            reordered_reads: self.reordered_reads.load(Ordering::Relaxed),
            reordered_batches: self.reordered_batches.load(Ordering::Relaxed),
            start_inversions,
            start_pairs,
            completion_inversions,
            completion_pairs,
            unused_started: u64::try_from(unused_started).unwrap_or(u64::MAX),
            first_need_ready: trace.first_need_ready,
            first_need_requested: trace.first_need_requested,
            first_need_unissued: trace.first_need_unissued,
            late_read_time,
            late_read_time_max,
            ready_lead_time,
        }
    }
}

/// Learn only actionable mistakes. Reads that were already ready retain the normal file-local
/// order; reads that missed their first-use deadline move ahead, longest observed lateness first
/// within the scheduling batch where they originated.
fn deadline_miss_order(trace: &IoOracleTrace) -> Vec<IoKey> {
    let mut order = Vec::new();
    for batch in &trace.started_batches {
        let mut late = batch
            .iter()
            .filter_map(|key| {
                let needed = trace.first_needed_at.get(key)?;
                let completed = trace.completed_at.get(key)?;
                (*completed > *needed).then(|| (*key, completed.duration_since(*needed)))
            })
            .collect::<Vec<_>>();
        late.sort_unstable_by(|(left_key, left_late), (right_key, right_late)| {
            right_late
                .cmp(left_late)
                .then_with(|| io_key_ordinal(*left_key).cmp(&io_key_ordinal(*right_key)))
        });
        order.extend(late.into_iter().map(|(key, _)| key));
    }
    order
}

fn io_key_ordinal(key: IoKey) -> u32 {
    match key {
        IoKey::Segment(id) => *id,
    }
}

fn sort_reads_by_key(reads: &mut [IoRead]) {
    reads.sort_unstable_by_key(|read| io_key_ordinal(read.key()));
}

fn order_inversions(order: &[IoKey], need_rank: &HashMap<IoKey, usize>) -> (u64, u64) {
    let ranks = order
        .iter()
        .filter_map(|key| need_rank.get(key).copied())
        .collect::<Vec<_>>();
    let mut tree = vec![0u64; need_rank.len().saturating_add(1)];
    let mut inversions = 0u64;
    for (seen, rank) in ranks.iter().copied().enumerate() {
        let before_or_equal = fenwick_sum(&tree, rank + 1);
        inversions = inversions.saturating_add(
            u64::try_from(seen)
                .unwrap_or(u64::MAX)
                .saturating_sub(before_or_equal),
        );
        fenwick_add(&mut tree, rank + 1);
    }
    let count = u64::try_from(ranks.len()).unwrap_or(u64::MAX);
    (
        inversions,
        count.saturating_mul(count.saturating_sub(1)) / 2,
    )
}

fn batch_order_inversions(batches: &[Vec<IoKey>], need_rank: &HashMap<IoKey, usize>) -> (u64, u64) {
    batches.iter().fold((0u64, 0u64), |totals, batch| {
        let (inversions, pairs) = order_inversions(batch, need_rank);
        (
            totals.0.saturating_add(inversions),
            totals.1.saturating_add(pairs),
        )
    })
}

fn completion_batch_inversions(
    completed: &[IoKey],
    started_batches: &[Vec<IoKey>],
    need_rank: &HashMap<IoKey, usize>,
) -> (u64, u64) {
    let batch_by_key = started_batches
        .iter()
        .enumerate()
        .flat_map(|(batch, keys)| keys.iter().copied().map(move |key| (key, batch)))
        .collect::<HashMap<_, _>>();
    let mut completion_batches = vec![Vec::new(); started_batches.len()];
    for key in completed {
        if let Some(&batch) = batch_by_key.get(key) {
            completion_batches[batch].push(*key);
        }
    }
    batch_order_inversions(&completion_batches, need_rank)
}

fn fenwick_sum(tree: &[u64], mut index: usize) -> u64 {
    let mut sum = 0u64;
    while index > 0 {
        sum = sum.saturating_add(tree[index]);
        index &= index - 1;
    }
    sum
}

fn fenwick_add(tree: &mut [u64], mut index: usize) {
    while index < tree.len() {
        tree[index] = tree[index].saturating_add(1);
        index += index & index.wrapping_neg();
    }
}

impl IoCell {
    fn priority(&self) -> IoPriority {
        if self.required.load(Ordering::Acquire) {
            IoPriority::Required
        } else {
            IoPriority::Speculative
        }
    }

    fn wake_waiters(&self) {
        for waiter in std::mem::take(&mut *self.waiters.lock()) {
            waiter.wake();
        }
    }
}

const PROBE_UNKNOWN: u8 = 0;
const PROBE_SUPPORTED: u8 = 1;
const PROBE_UNSUPPORTED: u8 = 2;

/// Scan-wide registry of raw segment requests.
///
/// A cell is created once per key per scan. Morsel-local planes hold references to these cells,
/// so two overlapping morsels share both an in-flight request and its completed bytes. The
/// service never performs a read itself: it emits [`IoDemand`] and waits for [`IoCompletions`].
pub(crate) struct IoService {
    demand: mpsc::UnboundedSender<IoDemand>,
    cells: Mutex<HashMap<IoKey, Arc<IoCell>>>,
    probe: Mutex<Option<NowaitProbe>>,
    probe_support: AtomicU8,
    background_reads: AtomicBool,
    io_bytes: AtomicU64,
    io_waits: AtomicU64,
    io_wait_nanos: AtomicU64,
    io_starts: AtomicU64,
    io_start_batches: AtomicU64,
    oracle: OnceLock<IoOracle>,
}

impl IoService {
    /// Create a service and the demand stream it will emit reads on.
    pub(crate) fn new() -> (Arc<Self>, IoDemandStream) {
        let (demand, stream) = mpsc::unbounded();
        let service = Arc::new(Self {
            demand,
            cells: Mutex::new(HashMap::default()),
            probe: Mutex::new(None),
            probe_support: AtomicU8::new(PROBE_UNKNOWN),
            background_reads: AtomicBool::new(true),
            io_bytes: AtomicU64::new(0),
            io_waits: AtomicU64::new(0),
            io_wait_nanos: AtomicU64::new(0),
            io_starts: AtomicU64::new(0),
            io_start_batches: AtomicU64::new(0),
            oracle: OnceLock::new(),
        });
        (service, stream)
    }

    pub(crate) fn completions(self: &Arc<Self>) -> IoCompletions {
        IoCompletions {
            service: Arc::downgrade(self),
        }
    }

    pub(crate) fn set_probe(&self, probe: Option<NowaitProbe>) {
        *self.probe.lock() = probe;
        self.probe_support.store(PROBE_UNKNOWN, Ordering::Release);
    }

    pub(crate) fn probe(&self) -> Option<NowaitProbe> {
        self.probe.lock().clone()
    }

    pub(crate) fn set_background_reads(&self, background: bool) {
        self.background_reads.store(background, Ordering::Release);
    }

    /// Whether planned reads should be started ahead of demand.
    ///
    /// Storage that overlaps and coalesces I/O wants every planned read as early as possible.
    /// In-memory sources keep this off so execution resolves cells inline through the probe.
    pub(crate) fn background_reads(&self) -> bool {
        self.background_reads.load(Ordering::Acquire)
    }

    pub(crate) fn probe_unsupported(&self) -> bool {
        self.probe_support.load(Ordering::Acquire) == PROBE_UNSUPPORTED
    }

    #[cfg(any(test, feature = "_test-harness"))]
    pub(crate) fn enable_oracle(&self, replay: impl IntoIterator<Item = IoKey>) {
        drop(self.oracle.set(IoOracle::new(replay)));
    }

    pub(crate) fn oracle_snapshot(&self) -> Option<IoOracleSnapshot> {
        self.oracle.get().map(IoOracle::snapshot)
    }

    pub(crate) fn sort_reads(&self, reads: &mut [IoRead]) {
        match self.oracle.get() {
            Some(oracle) => oracle.sort_reads(reads),
            None => sort_reads_by_key(reads),
        }
    }

    fn register(&self, key: IoKey, priority: IoPriority) -> (Arc<IoCell>, bool) {
        let mut cells = self.cells.lock();
        if let Some(cell) = cells.get(&key) {
            if priority == IoPriority::Required {
                cell.required.store(true, Ordering::Release);
            }
            return (Arc::clone(cell), false);
        }

        let cell = Arc::new(IoCell {
            key,
            state: Mutex::new(CellState::Unissued),
            waiters: Mutex::new(Vec::new()),
            required: AtomicBool::new(priority == IoPriority::Required),
            submitted: AtomicBool::new(false),
        });
        cells.insert(key, Arc::clone(&cell));
        (cell, true)
    }

    /// Register a scan-level lookahead set before morsel-local planning starts.
    pub(crate) fn register_reads(
        &self,
        keys: impl IntoIterator<Item = IoKey>,
        priority: IoPriority,
    ) -> Vec<IoRead> {
        keys.into_iter()
            .filter_map(|key| {
                let (cell, _) = self.register(key, priority);
                (!cell.submitted.swap(true, Ordering::AcqRel)).then_some(IoRead { cell })
            })
            .collect()
    }

    /// Hand every still-unissued read in `reads` out as one demand batch.
    ///
    /// Returns how many reads this call started. The batch boundary is preserved on the stream so
    /// the storage layer can coalesce neighbours that were planned together. If nobody is
    /// listening any more, the reads fail instead of leaving workers parked on them.
    pub(crate) fn start(&self, reads: &[IoRead]) -> usize {
        let mut requests = Vec::with_capacity(reads.len());
        let mut cells = Vec::with_capacity(reads.len());
        for read in reads {
            let mut state = read.cell.state.lock();
            if !matches!(*state, CellState::Unissued) {
                continue;
            }
            *state = CellState::Requested {
                started: Instant::now(),
            };
            requests.push(IoRequest {
                key: read.cell.key,
                priority: read.cell.priority(),
            });
            cells.push(Arc::clone(&read.cell));
        }
        if requests.is_empty() {
            return 0;
        }
        if let Some(oracle) = self.oracle.get() {
            oracle.observe_start(&requests);
        }
        let started = requests.len();
        self.io_starts.fetch_add(started as u64, Ordering::Relaxed);
        self.io_start_batches.fetch_add(1, Ordering::Relaxed);
        if self
            .demand
            .unbounded_send(IoDemand::Start(requests))
            .is_err()
        {
            for cell in &cells {
                self.fail_cell(cell);
            }
        }
        started
    }

    /// Mark a read as blocking execution: start it if nobody has, and ask for it to run ahead of
    /// speculative work.
    pub(crate) fn promote(&self, read: &IoRead) {
        read.cell.required.store(true, Ordering::Release);
        read.cell.submitted.store(true, Ordering::Release);
        // A read started here goes out as required and is polled at once; only a read someone
        // else already handed out as speculative needs the separate promotion.
        if self.start(std::slice::from_ref(read)) == 0
            && !read.is_settled()
            && self
                .demand
                .unbounded_send(IoDemand::Promote(read.cell.key))
                .is_err()
        {
            self.fail_cell(&read.cell);
        }
    }

    /// Settle a cell as failed because its demand can no longer be answered.
    fn fail_cell(&self, cell: &IoCell) {
        let mut state = cell.state.lock();
        if matches!(*state, CellState::Ready(_) | CellState::Failed(_)) {
            return;
        }
        *state = CellState::Failed("the scan's I/O demand stream is closed".into());
        drop(state);
        cell.wake_waiters();
    }

    pub(crate) fn read(&self, ticket: IoTicket) -> Option<IoRead> {
        self.read_key(ticket.key())
    }

    pub(crate) fn read_key(&self, key: IoKey) -> Option<IoRead> {
        self.cells
            .lock()
            .get(&key)
            .cloned()
            .map(|cell| IoRead { cell })
    }

    fn complete(&self, key: IoKey, result: VortexResult<BufferHandle>) {
        let Some(cell) = self.cells.lock().get(&key).cloned() else {
            return;
        };
        let mut state = cell.state.lock();
        let started = match &*state {
            CellState::Ready(_) | CellState::Failed(_) => return,
            CellState::Requested { started } => Some(*started),
            CellState::Unissued => None,
        };
        if let Some(oracle) = self.oracle.get() {
            oracle.observe_complete(key);
        }
        *state = match result {
            Ok(handle) => {
                self.io_bytes
                    .fetch_add(handle.len() as u64, Ordering::Relaxed);
                CellState::Ready(handle)
            }
            Err(err) => CellState::Failed(err.to_string().into()),
        };
        drop(state);
        if let Some(started) = started {
            self.io_waits.fetch_add(1, Ordering::Relaxed);
            self.io_wait_nanos.fetch_add(
                u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }
        cell.wake_waiters();
    }

    /// Reads handed out so far.
    pub(crate) fn io_starts(&self) -> u64 {
        self.io_starts.load(Ordering::Relaxed)
    }

    /// Demand batches handed out so far.
    pub(crate) fn io_start_batches(&self) -> u64 {
        self.io_start_batches.load(Ordering::Relaxed)
    }

    /// Bytes delivered through completions so far.
    pub(crate) fn io_bytes(&self) -> u64 {
        self.io_bytes.load(Ordering::Relaxed)
    }

    /// Reads that were handed out and answered through completions rather than resolved inline.
    pub(crate) fn io_waits(&self) -> u64 {
        self.io_waits.load(Ordering::Relaxed)
    }

    /// Total time handed-out reads spent outstanding.
    pub(crate) fn io_wait_time(&self) -> Duration {
        Duration::from_nanos(self.io_wait_nanos.load(Ordering::Relaxed))
    }
}

/// One registered read the scheduler can start, promote, or park on.
#[derive(Clone)]
pub(crate) struct IoRead {
    cell: Arc<IoCell>,
}

impl IoRead {
    pub(crate) fn key(&self) -> IoKey {
        self.cell.key
    }

    pub(crate) fn priority(&self) -> IoPriority {
        self.cell.priority()
    }

    pub(crate) fn require(&self) {
        self.cell.required.store(true, Ordering::Release);
        self.cell.submitted.store(true, Ordering::Release);
    }

    pub(crate) fn is_unissued(&self) -> bool {
        matches!(*self.cell.state.lock(), CellState::Unissued)
    }

    /// Whether the read has reached a terminal state.
    pub(crate) fn is_settled(&self) -> bool {
        matches!(
            *self.cell.state.lock(),
            CellState::Ready(_) | CellState::Failed(_)
        )
    }

    /// Subscribe an affinity-owned continuation to this exact cell.
    ///
    /// Returns `true` when the continuation was parked. The state lock closes the completion race:
    /// a completion either drains this waker or is observed here before insertion.
    pub(crate) fn park(&self, waker: Waker) -> bool {
        let state = self.cell.state.lock();
        if matches!(*state, CellState::Ready(_) | CellState::Failed(_)) {
            return false;
        }
        let mut waiters = self.cell.waiters.lock();
        if !waiters.iter().any(|waiter| waiter.will_wake(&waker)) {
            waiters.push(waker);
        }
        true
    }
}

/// The ticket view owned by one affinity-local morsel continuation.
///
/// The keyed map uses interior mutability because only planning and execution touch its shape.
/// Individual cells live in the scan-wide service and are synchronized because completions arrive
/// from outside the worker.
pub struct IoPlane {
    service: Arc<IoService>,
    probe: Option<NowaitProbe>,
    cells: RefCell<HashMap<IoKey, Arc<IoCell>>>,
    unsubmitted: RefCell<Vec<Arc<IoCell>>>,
}

impl IoPlane {
    /// Create a morsel-local view over the scan's shared IO service.
    pub(crate) fn new(service: Arc<IoService>) -> Self {
        let probe = service.probe();
        Self {
            service,
            probe,
            cells: RefCell::new(HashMap::default()),
            unsubmitted: RefCell::new(Vec::new()),
        }
    }

    /// Register a batch of uses, creating any cell that does not already exist.
    pub(crate) fn register(
        &self,
        batch: IoBatch,
        priority: IoPriority,
        stats: &mut ScanStats,
    ) -> VortexResult<Vec<IoTicket>> {
        let mut cells = self.cells.borrow_mut();
        let mut tickets = Vec::with_capacity(batch.uses().len());
        for r#use in batch.uses() {
            if !cells.contains_key(&r#use.key) {
                stats.io_registered += 1;
                let (cell, created) = self.service.register(r#use.key, priority);
                if !created {
                    stats.io_cell_hits += 1;
                }
                self.unsubmitted.borrow_mut().push(Arc::clone(&cell));
                cells.insert(r#use.key, cell);
            } else {
                stats.io_cell_hits += 1;
                if priority == IoPriority::Required {
                    cells[&r#use.key].required.store(true, Ordering::Release);
                }
            }
            tickets.push(IoTicket(r#use.key));
        }
        Ok(tickets)
    }

    /// Take newly registered reads for submission to the scheduler.
    ///
    /// Each cell is returned at most once even when planning spans several quanta. Duplicate
    /// logical uses retain one keyed cell and cannot submit duplicate reads.
    pub(crate) fn take_reads(&self) -> Vec<IoRead> {
        std::mem::take(&mut *self.unsubmitted.borrow_mut())
            .into_iter()
            .filter(|cell| {
                !matches!(*cell.state.lock(), CellState::Ready(_))
                    && !cell.submitted.swap(true, Ordering::AcqRel)
            })
            .map(|cell| IoRead { cell })
            .collect()
    }

    /// Resolve a ticket inline when the probe can prove the bytes are immediately available.
    ///
    /// On a miss the caller blocks on the ticket and the scheduler hands the read out as required
    /// demand when it parks the worker. A read this morsel planned has already been taken by its
    /// planning wave by then; the direct hand-out below only covers a cell nothing ever
    /// submitted. The cell is retained so duplicate uses inside this morsel share the same
    /// handle.
    pub(crate) fn ready(
        &self,
        ticket: IoTicket,
        stats: &mut ScanStats,
    ) -> VortexResult<Option<BufferHandle>> {
        let cell = self
            .cells
            .borrow()
            .get(&ticket.key())
            .cloned()
            .ok_or_else(|| vortex_err!("IO ticket was accessed without registration"))?;
        let mut state = cell.state.lock();
        if let Some(oracle) = self.service.oracle.get() {
            let first_need_state = match &*state {
                CellState::Ready(_) | CellState::Failed(_) => FirstNeedState::Ready,
                CellState::Requested { .. } => FirstNeedState::Requested,
                CellState::Unissued => FirstNeedState::Unissued,
            };
            oracle.observe_need(cell.key, first_need_state);
        }
        match &*state {
            CellState::Ready(handle) => return Ok(Some(handle.clone())),
            CellState::Requested { .. } => return Ok(None),
            CellState::Failed(error) => {
                return Err(vortex_err!("segment read failed: {error}"));
            }
            CellState::Unissued => {}
        }

        if let Some(probe) = &self.probe
            && !self.service.probe_unsupported()
        {
            stats.nowait_attempts += 1;
            match probe(cell.key)? {
                ReadAtNowait::Ready(handle) => {
                    self.service
                        .probe_support
                        .store(PROBE_SUPPORTED, Ordering::Release);
                    stats.nowait_hits += 1;
                    stats.io_bytes += handle.len() as u64;
                    *state = CellState::Ready(handle.clone());
                    drop(state);
                    cell.wake_waiters();
                    return Ok(Some(handle));
                }
                ReadAtNowait::WouldBlock => {
                    self.service
                        .probe_support
                        .store(PROBE_SUPPORTED, Ordering::Release);
                    stats.nowait_misses += 1;
                }
                ReadAtNowait::Unsupported => {
                    self.service
                        .probe_support
                        .store(PROBE_UNSUPPORTED, Ordering::Release);
                    stats.nowait_unsupported += 1;
                }
            }
        }

        // A read taken by a planning wave is started with that wave, so its neighbours coalesce,
        // and promoted when the worker parks on it. Anything never submitted is needed right now.
        if cell.submitted.load(Ordering::Acquire) {
            return Ok(None);
        }
        drop(state);
        self.service.promote(&IoRead {
            cell: Arc::clone(&cell),
        });
        Ok(None)
    }

    /// Drop every cell. Called between morsel batches to bound retained bytes.
    pub fn clear(&self) {
        self.cells.borrow_mut().clear();
        self.unsubmitted.borrow_mut().clear();
    }

    /// Drop the cell behind a key, if it is resolved.
    pub fn release(&self, key: IoKey) {
        self.cells.borrow_mut().remove(&key);
    }
}

/// Error helper for a ticket consumed without ever having been planned.
pub fn unplanned_ticket(producer: ProducerId) -> vortex_error::VortexError {
    vortex_err!(
        "node {} waited on a ticket its planning stream never emitted",
        producer.0
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use vortex_error::VortexResult;
    use vortex_error::vortex_err;
    use vortex_layout::segments::SegmentId;
    use vortex_utils::aliases::hash_map::HashMap;

    use super::FirstNeedState;
    use super::IoKey;
    use super::IoOracleTrace;
    use super::IoPriority;
    use super::IoService;
    use super::deadline_miss_order;
    use super::order_inversions;

    fn key(id: u32) -> IoKey {
        IoKey::Segment(SegmentId::from(id))
    }

    #[test]
    fn replay_order_is_applied_and_scored_against_first_use() -> VortexResult<()> {
        let (service, _demand) = IoService::new();
        service.enable_oracle([key(2), key(0)]);
        let mut reads = service.register_reads([key(0), key(1), key(2)], IoPriority::Speculative);

        service.sort_reads(&mut reads);
        assert_eq!(
            reads.iter().map(|read| read.key()).collect::<Vec<_>>(),
            [key(2), key(0), key(1)]
        );
        assert_eq!(service.start(&reads), 3);

        let oracle = service
            .oracle
            .get()
            .ok_or_else(|| vortex_err!("oracle was not enabled"))?;
        for needed in [key(2), key(0), key(1)] {
            oracle.observe_need(needed, FirstNeedState::Requested);
        }
        for completed in [key(0), key(2), key(1)] {
            oracle.observe_complete(completed);
        }

        let snapshot = oracle.snapshot();
        assert_eq!(snapshot.replay_keys, 2);
        assert_eq!(snapshot.replay_hits, 2);
        assert_eq!(snapshot.replay_misses, 1);
        assert_eq!(snapshot.reordered_reads, 3);
        assert_eq!(snapshot.reordered_batches, 1);
        assert_eq!((snapshot.start_inversions, snapshot.start_pairs), (0, 3));
        assert_eq!(
            (snapshot.completion_inversions, snapshot.completion_pairs),
            (1, 3)
        );
        assert_eq!(snapshot.first_need_requested, 3);
        assert_eq!(snapshot.unused_started, 0);
        Ok(())
    }

    #[test]
    fn inversion_count_ignores_unneeded_reads() {
        let need_rank = [(key(0), 0), (key(1), 1), (key(2), 2)]
            .into_iter()
            .collect::<HashMap<_, _>>();
        assert_eq!(
            order_inversions(&[key(2), key(9), key(0), key(1)], &need_rank),
            (2, 3)
        );
    }

    #[test]
    fn learned_order_contains_only_deadline_misses_longest_first() {
        let now = Instant::now();
        let trace = IoOracleTrace {
            started_batches: vec![vec![key(0), key(1), key(2)]],
            first_needed_at: [
                (key(0), now + Duration::from_millis(20)),
                (key(1), now + Duration::from_millis(10)),
                (key(2), now + Duration::from_millis(15)),
            ]
            .into_iter()
            .collect(),
            completed_at: [
                (key(0), now + Duration::from_millis(5)),
                (key(1), now + Duration::from_millis(40)),
                (key(2), now + Duration::from_millis(25)),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        assert_eq!(deadline_miss_order(&trace), [key(1), key(2)]);
    }
}
