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
        let started = requests.len();
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
        self.cells
            .lock()
            .get(&ticket.key())
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

    /// Drop every cell and its bytes once a run is over.
    pub(crate) fn clear(&self) {
        self.cells.lock().clear();
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

    /// Whether the read has reached a terminal state.
    pub(crate) fn is_settled(&self) -> bool {
        matches!(
            *self.cell.state.lock(),
            CellState::Ready(_) | CellState::Failed(_)
        )
    }

    /// The failure recorded for this read, if its completion was an error.
    pub(crate) fn failure(&self) -> Option<Arc<str>> {
        match &*self.cell.state.lock() {
            CellState::Failed(error) => Some(Arc::clone(error)),
            _ => None,
        }
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
                if created {
                    stats.io_requests += 1;
                } else {
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

    /// Clone the reads this morsel has established as required.
    pub(crate) fn required_reads(&self) -> Vec<IoRead> {
        self.cells
            .borrow()
            .values()
            .filter(|cell| cell.required.load(Ordering::Acquire))
            .cloned()
            .map(|cell| IoRead { cell })
            .collect()
    }

    /// Return the segment IDs named by this morsel in stable order.
    pub(crate) fn segment_ids(&self) -> Vec<u32> {
        let mut ids = self
            .cells
            .borrow()
            .keys()
            .map(|key| match key {
                IoKey::Segment(id) => **id,
            })
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        ids
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
