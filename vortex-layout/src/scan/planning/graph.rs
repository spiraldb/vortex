// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use vortex_array::ArrayRef;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_error::vortex_panic;
use vortex_io::request;
use vortex_io::request::IoBatch;
use vortex_io::request::IoIntent;
use vortex_io::request::IoResult;
use vortex_mask::Mask;
use vortex_scan::planning::planner::State;
use vortex_session::VortexSession;

use crate::plan::physical::PlanRef;
use crate::plan::physical::pipeline::ReadId;
use crate::plan::physical::pipeline::Scan;
use crate::plan::physical::pipeline::Split;
use crate::plan::physical::pipeline::Turn;
use crate::plan::physical::pipeline::ValueCache;
use crate::scan::planning::SegmentLocation;
use crate::segments::SegmentId;

/// Rows a running plan produced, in row order.
#[derive(Clone)]
pub(crate) struct Piece {
    /// Orders the piece among the pieces of one graph.
    pub(crate) rows: Range<u64>,
    /// The selected rows the piece holds.
    pub(crate) array: ArrayRef,
}

/// A plan running over one row range on the pipeline [`Scan`].
///
/// The scan produces the selected rows in row order and leaves its reads to its owner.
pub(crate) struct ScanGraph {
    scan: Scan,
    /// A step the scan already took that the owner has not seen.
    pending: Option<Turn>,
    done: bool,
    /// Orders the pieces handed out.
    next_piece: u64,
}

/// What a graph wants from its owner.
enum GraphState {
    NeedsCompute,
    Waiting,
    Done,
}

impl ScanGraph {
    /// Compiles nothing yet: the scan compiles `plan` over `rows`, restricted to `mask`, on its
    /// first step. `row_offset` is the global row index of the plan's first row, and `values` the
    /// whole values shared with the split's other scans.
    pub(crate) fn try_new(
        session: VortexSession,
        plan: &PlanRef,
        rows: Range<u64>,
        mask: Mask,
        row_offset: u64,
        values: ValueCache,
    ) -> VortexResult<Self> {
        let scan = Scan::try_new(session, plan.clone(), vec![Split { rows, mask }])?
            .with_row_offset(row_offset)
            .with_value_cache(values);
        Ok(Self {
            scan,
            pending: None,
            done: false,
            next_piece: 0,
        })
    }

    fn state(&self) -> GraphState {
        match &self.pending {
            _ if self.done => GraphState::Done,
            Some(Turn::Waiting) => GraphState::Waiting,
            _ => GraphState::NeedsCompute,
        }
    }

    fn step(&mut self) -> VortexResult<Turn> {
        let turn = match self.pending.take() {
            Some(Turn::Waiting) | None => self.scan.step()?,
            Some(turn) => turn,
        };
        match &turn {
            Turn::Done => self.done = true,
            Turn::Waiting => self.pending = Some(Turn::Waiting),
            Turn::Read(_) | Turn::Output(..) => {}
        }
        Ok(turn)
    }

    fn piece(&mut self, array: ArrayRef) -> Piece {
        let start = self.next_piece;
        self.next_piece += 1;
        Piece {
            rows: start..start + 1,
            array,
        }
    }

    fn deliver(&mut self, read: ReadId, bytes: BufferHandle) -> VortexResult<()> {
        self.scan.deliver(read, bytes)?;
        if matches!(self.pending, Some(Turn::Waiting)) {
            self.pending = None;
        }
        Ok(())
    }
}

/// What one [`ProtocolGraph::compute`] produced.
pub(crate) enum GraphStep {
    /// Control returns to the owner with CPU work still ready.
    Yield,
    /// Requests to publish, each returned once.
    NeedsIO(IoBatch),
    /// Rows the plan produced.
    Piece(Piece),
}

/// A [`ScanGraph`] whose segment reads are published as protocol requests.
///
/// The scan names segments by id; the protocol names byte ranges. Each scan read becomes a
/// `Fetch` of the segment's range at the segment's alignment, and each delivery is handed back to
/// the scan under its own id.
pub(crate) struct ProtocolGraph {
    graph: ScanGraph,
    locations: Arc<[SegmentLocation]>,
    /// Published and undelivered requests, with the scan read and segment each one answers.
    outstanding: FxHashMap<u32, (ReadId, SegmentId)>,
    /// A delivery the scan could not take, reported by the next compute.
    failed: Option<VortexError>,
    /// The protocol id the next published request gets.
    next_id: u32,
}

impl ProtocolGraph {
    /// Wraps `graph`, numbering its requests from `first_id`.
    ///
    /// Request ids must be unique over an owner's lifetime, so an owner that runs several graphs
    /// starts each where the previous one's [`next_id`](Self::next_id) left off.
    pub(crate) fn new(graph: ScanGraph, locations: Arc<[SegmentLocation]>, first_id: u32) -> Self {
        Self {
            graph,
            locations,
            outstanding: FxHashMap::default(),
            failed: None,
            next_id: first_id,
        }
    }

    /// The protocol id the next request would get: the first id a following graph of the same
    /// owner may use.
    pub(crate) fn next_id(&self) -> u32 {
        self.next_id
    }

    /// The owner's protocol state while the graph is running.
    pub(crate) fn state(&self) -> State {
        if self.failed.is_some() {
            return State::NeedsCompute;
        }
        match self.graph.state() {
            GraphState::Done => State::Done,
            GraphState::NeedsCompute => State::NeedsCompute,
            GraphState::Waiting => State::Waiting,
        }
    }

    pub(crate) fn compute(&mut self) -> VortexResult<GraphStep> {
        if let Some(error) = self.failed.take() {
            return Err(error);
        }
        Ok(match self.graph.step()? {
            Turn::Waiting | Turn::Done => GraphStep::Yield,
            Turn::Output(_, array) => GraphStep::Piece(self.graph.piece(array)),
            Turn::Read(read) => {
                // Publish every read the scan has ready together, so the owner can coalesce them.
                let mut batch = vec![self.request(read.id, read.segment_id)?];
                loop {
                    match self.graph.step()? {
                        Turn::Read(read) => batch.push(self.request(read.id, read.segment_id)?),
                        turn => {
                            if !matches!(turn, Turn::Waiting) {
                                self.graph.pending = Some(turn);
                                self.graph.done = false;
                            }
                            break;
                        }
                    }
                }
                GraphStep::NeedsIO(batch)
            }
        })
    }

    fn request(&mut self, read: ReadId, segment_id: SegmentId) -> VortexResult<request::IoRequest> {
        let location = self.location(segment_id)?;
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| vortex_err!("ProtocolGraph ran out of request ids"))?;
        self.outstanding.insert(id, (read, segment_id));
        Ok(request::IoRequest {
            intent: IoIntent::Fetch,
            request: request::IoRequestId(id),
            target: location.target(),
        })
    }

    /// Hands a delivered read to the scan. The driver only delivers requests this graph
    /// published, so anything else is a driver bug. Bytes the scan cannot take fail the owner's
    /// next compute.
    pub(crate) fn set_io_result(&mut self, id: request::IoRequestId, result: IoResult) {
        let Some((read, segment_id)) = self.outstanding.remove(&id.0) else {
            vortex_panic!("ProtocolGraph: delivery of {id:?}, which is not outstanding");
        };
        let IoResult::Bytes(bytes) = result else {
            vortex_panic!("ProtocolGraph: segment {segment_id} answered with a size");
        };
        if let Err(error) = self.deliver(read, segment_id, bytes)
            && self.failed.is_none()
        {
            self.failed = Some(error.with_context(format!("delivering segment {segment_id}")));
        }
    }

    fn deliver(
        &mut self,
        read: ReadId,
        segment_id: SegmentId,
        bytes: BufferHandle,
    ) -> VortexResult<()> {
        // A source that honours the target's alignment makes this a check, not a copy.
        let alignment = self.location(segment_id)?.alignment;
        let bytes = BufferHandle::new_host(bytes.try_into_host_sync()?.aligned(alignment));
        self.graph.deliver(read, bytes)
    }

    fn location(&self, id: SegmentId) -> VortexResult<SegmentLocation> {
        self.locations
            .get(*id as usize)
            .copied()
            .ok_or_else(|| vortex_err!("segment {id} has no known location"))
    }
}
