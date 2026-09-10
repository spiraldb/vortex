// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The [`Operator`] contract, the context it runs under, and the per-worker tree.
//!
//! An operator owns its children and calls them directly. It is a state machine driven by three
//! calls per morsel: `look_ahead` names the reads its subtree needs, `next` produces its value, and
//! `close` releases what it held. A call that cannot make progress pushes the tickets it needs
//! into the context and returns `Blocked`; the worker parks on exactly those cells and calls
//! again, and the operator resumes from its own state. Nothing here is a future or a waker.
//!
//! Each tree belongs to one morsel. Constructors initialize operator state; repeated calls
//! resume that state until the morsel is closed and the tree is dropped.

use std::ops::Range;

use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::IntoArray;
use vortex_array::arrays::Chunked;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::Struct;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_layout::plan::ExactPlan;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;

use crate::cells::SharedCells;
use crate::demand::RowDomain;
use crate::io::IoKey;
use crate::io::IoPlane;
use crate::io::IoPriority;
use crate::io::IoTicket;
use crate::io::IoUse;
use crate::stats::HintSummary;
use crate::stats::NoReadReason;
use crate::stats::PollOutcome;
use crate::stats::ScanStats;
use crate::stats::TraceEventKind;
use crate::stats::TracePhase;
use crate::stats::ValueSummary;
use crate::stats::rows;
use crate::tee::MaskBuffer;

/// An operator's pre-order position in its tree. Names the operator in traces and I/O
/// attribution; never used for dispatch.
pub type NodeId = u32;

/// A value produced by an operator for its parent.
#[derive(Clone)]
pub enum Value {
    /// Dense rows, one per row of the batch's coverage, or exactly the selected rows below a
    /// filter.
    Array(ArrayRef),
    /// A selection over the batch's whole coverage; same length as the coverage.
    Mask(Mask),
}

impl Value {
    /// Unwrap an array value, or fail if this is a mask.
    pub fn into_array(self) -> VortexResult<ArrayRef> {
        match self {
            Value::Array(array) => Ok(array),
            Value::Mask(_) => Err(vortex_err!("expected an array value, got a mask")),
        }
    }

    /// Unwrap a mask value, or fail if this is an array.
    pub fn into_mask(self) -> VortexResult<Mask> {
        match self {
            Value::Mask(mask) => Ok(mask),
            Value::Array(_) => Err(vortex_err!("expected a mask value, got an array")),
        }
    }
}

/// A value plus the root rows it accounts for.
///
/// Below a filter a batch holds exactly the rows the filter keeps, in order; elsewhere it is
/// dense over its coverage. The row hint an operator runs under never changes that: a leaf that
/// was told nothing in its range is wanted stands in placeholder rows rather than leaving a hole.
/// Every sibling under one parent sees the same selection, so parents concatenate and zip
/// without any bookkeeping either way.
pub struct Batch {
    /// The root-coordinate row range this batch accounts for.
    pub coverage: Range<u64>,
    /// The value itself.
    pub value: Value,
}

impl Batch {
    /// An array batch.
    pub fn array(coverage: Range<u64>, array: ArrayRef) -> Self {
        Self {
            coverage,
            value: Value::Array(array),
        }
    }

    /// A mask batch.
    pub fn mask(coverage: Range<u64>, mask: Mask) -> Self {
        Self {
            coverage,
            value: Value::Mask(mask),
        }
    }
}

/// The result of pulling an operator.
pub enum Step {
    /// A value covering a dense input row range.
    Batch(Batch),
    /// Nothing more this morsel.
    Finished,
    /// The operator pushed the tickets it needs into the context; call again once they complete.
    Blocked,
}

/// Whether look-ahead has named every currently discoverable read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LookAhead {
    /// Every read this subtree needs has been named.
    Complete,
    /// More reads depend on the tickets added to the context. Resume after a dependency settles.
    Blocked,
}

impl LookAhead {
    /// Combine child results after visiting all independently runnable children.
    pub fn merge(self, other: Self) -> Self {
        if self == Self::Blocked || other == Self::Blocked {
            Self::Blocked
        } else {
            Self::Complete
        }
    }
}

/// Something a worker can park on.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Wait {
    /// An IO ticket an operator registered through the context.
    Io(IoTicket),
}

/// A set of [`Wait`]s. Small by construction — a morsel parks on the handful of cells it named.
#[derive(Clone, Debug, Default)]
pub struct WaitSet(Vec<Wait>);

impl WaitSet {
    /// An empty wait set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Park on one more thing.
    pub fn push(&mut self, wait: Wait) {
        self.0.push(wait);
    }

    /// The waits in this set.
    pub fn waits(&self) -> &[Wait] {
        &self.0
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<Wait> for WaitSet {
    fn from_iter<T: IntoIterator<Item = Wait>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// A per-morsel state machine that owns its children.
///
/// A tree is built for one morsel on the worker that drives it and dropped when it finishes.
/// It never leaves the thread, so nothing here is `Send`.
pub trait Operator {
    /// The row domain supplied when this operator was constructed.
    fn row_domain(&self) -> &RowDomain;

    /// Register discoverable reads, attaching live demand views from this operator's domain.
    ///
    /// A missing dependency is `cx.wait(ticket)` plus `LookAhead::Blocked`. Parents visit all
    /// independent children before returning blocked. Completed children need no further calls.
    /// There are no planning budgets or yields, and this method must never wait on storage.
    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead>;

    /// Produce the next value under `hint`.
    ///
    /// May resolve an unissued read inline through [`Cx::ready`], which the source guarantees
    /// will not wait on storage. Must not perform blocking IO, poll a future, or wait for an
    /// external resource: a missing dependency is [`Cx::wait`] plus [`Step::Blocked`].
    fn next(&mut self, hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step>;

    /// Release everything held for the finished morsel.
    fn close(&mut self, cx: &mut Cx<'_>);

    /// One line naming this operator, for traces. The tree appends its construction-time row range.
    fn describe(&self) -> String;
}

/// The root rows and selection of one morsel.
pub struct MorselRows<'a> {
    /// Root coordinates.
    pub range: Range<u64>,
    /// The rows the caller asked for, one entry per row of `range`. This is the selection every
    /// filter starts from; the conjuncts only ever narrow it.
    pub demand: &'a Mask,
}

/// What an operator can reach while it runs: the morsel, the I/O plane, the shared caches, and
/// the mask buffers. Operators push into it what is not a value; values come back as batches.
pub struct Cx<'a> {
    /// The morsel being driven.
    pub morsel: MorselRows<'a>,
    /// One per tee in the plan, owned by the worker, cleared per morsel. The root writes them,
    /// filters read them.
    pub tees: &'a mut [MaskBuffer],
    io: &'a IoPlane,
    cells: &'a SharedCells,
    dictionaries: &'a Mutex<HashMap<ExactPlan, ArrayRef>>,
    session: &'a VortexSession,
    stats: &'a mut ScanStats,
    priority: IoPriority,
    waits: WaitSet,
}

impl<'a> Cx<'a> {
    /// Add a read with its live demand to the context, returning its dependency ticket.
    pub fn register(&mut self, request: IoUse) -> VortexResult<IoTicket> {
        self.stats.io_uses += 1;
        self.io.register(request, self.priority, self.stats)
    }

    /// Whether a shared cell already holds the decoded value for a unit.
    ///
    /// A hit lets an operator skip naming the read: the morsel's own lease, counted into the
    /// cell before the scan started, keeps the value alive until it closes.
    pub fn decoded_available(&self, key: IoKey) -> bool {
        self.cells.decoded(key).is_some()
    }

    /// Record, for the trace, that an operator will not read `key` this morsel.
    pub fn note_no_read(&mut self, key: IoKey, reason: NoReadReason) {
        let IoKey::Segment(segment) = key;
        self.stats.record_event(TraceEventKind::NoRead {
            segment: *segment,
            reason,
        });
    }

    /// Run `f` with reads registered under `priority`.
    pub fn with_priority<T>(&mut self, priority: IoPriority, f: impl FnOnce(&mut Self) -> T) -> T {
        let previous = std::mem::replace(&mut self.priority, priority);
        let result = f(self);
        self.priority = previous;
        result
    }

    /// Clone ready bytes, first attempting a source-provided non-blocking inline read if unissued.
    pub fn ready(&mut self, ticket: IoTicket) -> VortexResult<Option<BufferHandle>> {
        self.io.ready(ticket, self.stats)
    }

    /// Park on this ticket. Push it, then return `Blocked`.
    pub fn wait(&mut self, ticket: IoTicket) {
        self.waits.push(Wait::Io(ticket));
    }

    /// Take a decoded value from the shared cell for a unit, if a morsel already published one.
    pub fn shared_decoded(&mut self, key: IoKey) -> Option<ArrayRef> {
        let hit = self.cells.decoded(key);
        if hit.is_some() {
            let IoKey::Segment(segment) = key;
            self.stats.record_decode_reuse(*segment);
        }
        hit
    }

    /// Publish a decoded value into the shared cell for a unit.
    pub fn publish_decoded(&self, key: IoKey, array: &ArrayRef) {
        self.cells.publish(key, array);
    }

    /// Dictionary values decoded earlier in this scan for the same values plan allocation.
    pub fn dictionary(&self, values: &ExactPlan) -> Option<ArrayRef> {
        self.dictionaries.lock().get(values).cloned()
    }

    /// Publish dictionary values for reuse during this scan. First writer wins.
    pub fn publish_dictionary(&self, values: ExactPlan, array: ArrayRef) -> ArrayRef {
        self.dictionaries
            .lock()
            .entry(values)
            .or_insert(array)
            .clone()
    }

    /// The session, for creating expression execution contexts.
    pub fn session(&self) -> &VortexSession {
        self.session
    }

    /// Mutable access to the run's counters.
    pub fn stats(&mut self) -> &mut ScanStats {
        self.stats
    }

    /// Release this morsel's lease on a unit, dropping the shared cell at the last release.
    pub fn release_use(&mut self, key: IoKey) {
        let IoKey::Segment(segment) = key;
        self.stats
            .record_event(TraceEventKind::Release { segment: *segment });
        self.cells.release(key);
    }
}

/// An owned child: an operator plus its trace id, so every call into it is traced under a
/// stable name.
pub struct Child {
    id: NodeId,
    op: Box<dyn Operator>,
    planned: bool,
}

impl Child {
    /// Wrap an operator under trace id `id`.
    pub fn new(id: NodeId, op: Box<dyn Operator>) -> Self {
        Self {
            id,
            op,
            planned: false,
        }
    }

    /// This child's trace id.
    pub fn id(&self) -> NodeId {
        self.id
    }

    fn label(&self) -> String {
        format!(
            "{} range {}",
            self.op.describe(),
            rows(self.row_domain().range())
        )
    }

    /// This child's construction-time row domain.
    pub fn row_domain(&self) -> &RowDomain {
        self.op.row_domain()
    }

    /// Resume look-ahead, or return complete if this child already finished it.
    pub fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        if self.planned {
            return Ok(LookAhead::Complete);
        }
        let traced = cx.stats.tracing();
        if traced {
            cx.stats.record_enter(
                self.id,
                TracePhase::Plan,
                self.label(),
                hint_summary(&self.row_domain().snapshot()),
            );
        }
        let result = self.op.look_ahead(cx)?;
        self.planned = result == LookAhead::Complete;
        if traced {
            cx.stats.record_return(match result {
                LookAhead::Complete => PollOutcome::PlanComplete,
                LookAhead::Blocked => PollOutcome::PlanBlocked(wait_keys(&cx.waits)),
            });
        }
        Ok(result)
    }

    /// Pull the child under `hint`. See [`Operator::next`].
    pub fn next(&mut self, hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        let traced = cx.stats.tracing();
        if traced {
            cx.stats.record_enter(
                self.id,
                TracePhase::Execute,
                self.label(),
                hint_summary(hint),
            );
        }
        let step = self.op.next(hint, cx);
        if traced && let Ok(step) = &step {
            cx.stats.record_return(match step {
                Step::Batch(batch) => PollOutcome::ExecuteValue {
                    coverage: batch.coverage.clone(),
                    value: match &batch.value {
                        Value::Array(array) => ValueSummary::Array {
                            rows: array.len(),
                            dtype: array.dtype().to_string(),
                        },
                        Value::Mask(mask) => ValueSummary::Mask {
                            selected: mask.true_count(),
                            rows: mask.len(),
                        },
                    },
                },
                Step::Blocked => PollOutcome::ExecuteBlocked(wait_keys(&cx.waits)),
                Step::Finished => PollOutcome::ExecuteFinished,
            });
        }
        step
    }

    /// Close the child. See [`Operator::close`].
    pub fn close(&mut self, cx: &mut Cx<'_>) {
        let traced = cx.stats.tracing();
        if traced {
            cx.stats.record_retire_enter(self.id, self.label());
        }
        self.op.close(cx);
        if traced {
            cx.stats.record_retire_exit();
        }
    }
}

/// One morsel's operator tree and the mask buffers its filters read.
///
/// Built and dropped on the worker that drives the morsel; it never crosses a thread.
pub struct Tree {
    root: Child,
    tees: Vec<MaskBuffer>,
}

/// What the driver lends the tree for one call.
pub(crate) struct Env<'a> {
    pub io: &'a IoPlane,
    pub cells: &'a SharedCells,
    pub dictionaries: &'a Mutex<HashMap<ExactPlan, ArrayRef>>,
    pub session: &'a VortexSession,
    pub stats: &'a mut ScanStats,
}

impl Tree {
    pub(crate) fn new(root: Child, tees: usize, morsel_start: u64) -> Self {
        Self {
            root,
            tees: (0..tees).map(|_| MaskBuffer::new(morsel_start)).collect(),
        }
    }

    fn cx<'a>(
        &'a mut self,
        range: Range<u64>,
        demand: &'a Mask,
        env: Env<'a>,
    ) -> (&'a mut Child, Cx<'a>) {
        let Tree { root, tees } = self;
        let cx = Cx {
            morsel: MorselRows { range, demand },
            tees,
            io: env.io,
            cells: env.cells,
            dictionaries: env.dictionaries,
            session: env.session,
            stats: env.stats,
            priority: IoPriority::Required,
            waits: WaitSet::new(),
        };
        (root, cx)
    }

    /// Resume look-ahead and return the dependencies that prevented further planning.
    pub(crate) fn look_ahead(
        &mut self,
        range: Range<u64>,
        demand: &Mask,
        env: Env<'_>,
    ) -> VortexResult<(LookAhead, WaitSet)> {
        let (root, mut cx) = self.cx(range, demand, env);
        let result = root.look_ahead(&mut cx)?;
        Ok((result, cx.waits))
    }

    /// Pull the root once, returning the tickets it parked on when blocked.
    pub(crate) fn next(
        &mut self,
        range: Range<u64>,
        demand: &Mask,
        env: Env<'_>,
    ) -> VortexResult<(Step, WaitSet)> {
        let (root, mut cx) = self.cx(range, demand, env);
        let step = root.next(demand, &mut cx)?;
        Ok((step, cx.waits))
    }

    /// Close the root, releasing every lease the morsel held.
    pub(crate) fn close(&mut self, range: Range<u64>, demand: &Mask, env: Env<'_>) {
        let (root, mut cx) = self.cx(range, demand, env);
        root.close(&mut cx);
    }
}

fn hint_summary(hint: &Mask) -> HintSummary {
    HintSummary {
        rows: hint.len(),
        selected: hint.true_count(),
    }
}

pub(crate) fn wait_keys(waits: &WaitSet) -> Vec<IoKey> {
    waits
        .waits()
        .iter()
        .map(|Wait::Io(ticket)| ticket.key())
        .collect()
}

/// Keep exactly the rows `keep` selects, one chunk at a time.
///
/// The generic filter kernel turns a sparse mask over a chunked array into per-index takes,
/// which cost Q15 about 15 percent. Filtering each chunk by its own slice of the mask keeps the
/// cost profile the leaves have when they filter themselves.
pub(crate) fn filter_rows(array: ArrayRef, keep: Mask) -> VortexResult<ArrayRef> {
    if keep.all_true() {
        return Ok(array);
    }
    if keep.all_false() {
        return Ok(Canonical::empty(array.dtype()).into_array());
    }
    if let Some(chunked) = array.as_opt::<Chunked>() {
        let dtype = array.dtype().clone();
        let mut parts = Vec::with_capacity(chunked.nchunks());
        let mut offset = 0usize;
        for chunk in chunked.iter_chunks() {
            let end = offset + chunk.len();
            let part = keep.slice(offset..end);
            if !part.all_false() {
                parts.push(filter_rows(chunk.clone(), part)?);
            }
            offset = end;
        }
        return concat_parts(parts, &dtype);
    }
    if let Some(struct_) = array.as_opt::<Struct>() {
        let len = keep.true_count();
        let validity = struct_.struct_validity().filter(&keep)?;
        let fields = struct_
            .iter_unmasked_fields()
            .map(|field| filter_rows(field.clone(), keep.clone()))
            .collect::<VortexResult<Vec<_>>>()?;
        return Ok(
            StructArray::try_new(struct_.names().clone(), fields, len, validity)?.into_array(),
        );
    }
    array.filter(keep)
}

/// Concatenate parts in order: empty, the single part, or a chunked array.
pub(crate) fn concat_parts(mut parts: Vec<ArrayRef>, dtype: &DType) -> VortexResult<ArrayRef> {
    Ok(match parts.len() {
        0 => Canonical::empty(dtype).into_array(),
        1 => parts.pop().vortex_expect("one part"),
        _ => ChunkedArray::try_new(parts, dtype.clone())?.into_array(),
    })
}
