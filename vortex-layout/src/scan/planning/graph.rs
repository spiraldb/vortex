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
use vortex_io::segments::IoIntent;
use vortex_io::segments::SegmentKey;
use vortex_io::segments::SegmentRequest;
use vortex_io::segments::SourceId;
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
    NeedsIO(Vec<SegmentRequest>),
    /// Rows the plan produced.
    Piece(Piece),
}

/// A [`ScanGraph`] whose segment reads are published as protocol requests.
///
/// Each scan read becomes a `Fetch` of its segment in `source`, and each delivery is handed back to
/// the scan under the ids of every read of that segment. A segment the scan reads twice while the
/// first read is outstanding is fetched once.
pub(crate) struct ProtocolGraph {
    graph: ScanGraph,
    source: SourceId,
    locations: Arc<[SegmentLocation]>,
    /// Fetched and undelivered segments, with the scan reads each one answers.
    outstanding: FxHashMap<SegmentId, Vec<ReadId>>,
    /// A delivery the scan could not take, reported by the next compute.
    failed: Option<VortexError>,
}

impl ProtocolGraph {
    /// Wraps `graph`, fetching its segments from `source`.
    pub(crate) fn new(
        graph: ScanGraph,
        source: SourceId,
        locations: Arc<[SegmentLocation]>,
    ) -> Self {
        Self {
            graph,
            source,
            locations,
            outstanding: FxHashMap::default(),
            failed: None,
        }
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
                // Publish every read the scan has ready together, so the service can coalesce them.
                let mut batch = Vec::new();
                self.request(read.id, read.segment_id, &mut batch);
                loop {
                    match self.graph.step()? {
                        Turn::Read(read) => self.request(read.id, read.segment_id, &mut batch),
                        turn => {
                            if !matches!(turn, Turn::Waiting) {
                                self.graph.pending = Some(turn);
                                self.graph.done = false;
                            }
                            break;
                        }
                    }
                }
                if batch.is_empty() {
                    GraphStep::Yield
                } else {
                    GraphStep::NeedsIO(batch)
                }
            }
        })
    }

    fn request(&mut self, read: ReadId, segment: SegmentId, batch: &mut Vec<SegmentRequest>) {
        let reads = self.outstanding.entry(segment).or_default();
        if reads.is_empty() {
            batch.push(SegmentRequest::new(
                SegmentKey::new(self.source, segment),
                IoIntent::Fetch,
            ));
        }
        reads.push(read);
    }

    /// Hands a fetched segment to every scan read of it. The driver only delivers segments this
    /// graph fetched. Bytes the scan cannot take fail the owner's next compute.
    pub(crate) fn deliver(&mut self, key: SegmentKey, bytes: BufferHandle) {
        let result = self.deliver_segment(key.segment, bytes);
        if let Err(error) = result
            && self.failed.is_none()
        {
            self.failed = Some(error.with_context(format!("delivering segment {}", key.segment)));
        }
    }

    fn deliver_segment(&mut self, segment: SegmentId, bytes: BufferHandle) -> VortexResult<()> {
        let reads = self.outstanding.remove(&segment).ok_or_else(|| {
            vortex_err!("delivery of segment {segment}, which is not outstanding")
        })?;
        // A source that honours the segment's alignment makes this a check, not a copy.
        let alignment = self.location(segment)?.alignment;
        let bytes = BufferHandle::new_host(bytes.try_into_host_sync()?.aligned(alignment));
        for read in reads {
            self.graph.deliver(read, bytes.clone())?;
        }
        Ok(())
    }

    fn location(&self, id: SegmentId) -> VortexResult<SegmentLocation> {
        self.locations
            .get(*id as usize)
            .copied()
            .ok_or_else(|| vortex_err!("segment {id} has no known location"))
    }
}
