// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Push-based execution of physical plans as pipelines.
//!
//! A [`Scan`] runs a [`PlanRef`](crate::plan::PlanRef) over a list of splits, row ranges of the
//! plan's domain. Each split is compiled into pipelines: a pipeline is one [`Source`], zero or
//! more [`Operator`]s, and the ports its last stage writes. The driver asks the source for a batch
//! and pushes it through the stages into the ports. Every port runs from one pipeline's sink,
//! the end that pushes into its outlets, to another's source, which reads it as an inlet, and
//! the plan joining them declares how many batches each inlet may hold.
//!
//! A source checks its readiness before computing, waiting for a segment read or an inlet.
//! The driver also blocks a pipeline when an outlet is full. The scheduler re-runs a blocked
//! pipeline when that one condition clears.
//!
//! The owner of a scan answers reads: [`Scan::step`] returns each read as a [`Turn::Read`], and
//! [`Scan::deliver`] hands its bytes back. Nothing in a scan awaits.

mod compile;
mod port;
mod scan;
pub mod synthetic;

use std::collections::VecDeque;

use smallvec::SmallVec;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexResult;
use vortex_session::VortexSession;

pub use self::compile::Chain;
pub use self::compile::Compiler;
pub use self::compile::Reach;
use self::port::Arena;
pub use self::port::Inlet;
use self::port::PortId;
pub use self::scan::ReadId;
pub use self::scan::ReadRequest;
pub use self::scan::Scan;
pub use self::scan::Split;
pub use self::scan::Turn;
use crate::segments::SegmentId;

/// Batches an inlet holds before its sink is blocked, unless the plan joining it says
/// otherwise; see [`Compiler::join_with`].
pub const DEFAULT_CAPACITY: usize = 8;

/// The output of one non-blocking compute call.
#[derive(Debug)]
pub enum Step {
    /// No output remains for this input. A source has finished entirely.
    End,
    /// A batch with more computation available without another input or readiness check.
    More(ArrayRef),
    /// The last batch for this input. A stage needs another input; a source needs readiness
    /// checked again.
    Final(ArrayRef),
}

/// Whether a source can compute or needs the scheduler to wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceReady {
    /// The next compute call can produce output or finish without blocking.
    Ready,
    /// New reads were queued through [`Cx::request_read`]. Run again when bytes are available.
    Read,
    /// Nothing can happen until the condition clears.
    Blocked(SourceBlocked),
}

/// Why a source cannot compute yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceBlocked {
    /// A read was requested and has not been delivered.
    Io,
    /// The inlet needs more batches or its sink to close it. It must be open and have room
    /// for its sink to push; debug builds check that the sink can still wake this source.
    Inlet(usize),
}

/// The start of a pipeline: produces batches from its inlets and the segments it asks to have
/// read.
pub trait Source: Operator {
    /// Prepares the next [`compute`](Operator::compute) call, after the driver checks outlets.
    /// Returning [`SourceReady::Ready`] guarantees that call can proceed without blocking.
    /// Any input or choice made here can be retained by the source for that call.
    ///
    /// Queue one or more reads with [`Cx::request_read`] and return [`SourceReady::Read`] to
    /// issue them. Readiness is checked again when bytes are available, including bytes already
    /// delivered; they are available through [`Cx::take_segment`] in arrival order.
    fn ready(&mut self, _cx: &mut Cx<'_>) -> VortexResult<SourceReady> {
        Ok(SourceReady::Ready)
    }
}

/// What an operator is handed by the stage below it.
#[derive(Debug)]
pub enum Input {
    /// No new input: a source is ready, or an operator is continuing after [`Step::More`].
    None,
    /// The next batch.
    Batch(ArrayRef),
    /// The stage below has finished: no more batches come. Handed over once; [`Input::None`]
    /// may follow to drain any [`Step::More`] outputs.
    End,
}

/// Computes batches without blocking, either as a source or as a stage transforming input.
pub trait Operator: Send {
    /// Computes one output. [`Step::More`] requests another call with [`Input::None`] before
    /// any new input is supplied. [`Step::End`] and [`Step::Final`] finish the current input;
    /// after [`Input::End`], they finish the stage as well.
    ///
    /// Sources receive only [`Input::None`]. Their first call and each call after
    /// [`Step::Final`] follow [`Source::ready`]; [`Step::End`] finishes the source.
    fn compute(&mut self, input: Input, cx: &mut Cx<'_>) -> VortexResult<Step>;
}

/// Distinct inlet indices, with constant-time insertion and removal.
#[derive(Default)]
struct InletSet {
    indices: Vec<usize>,
    positions: Vec<usize>,
}

impl InletSet {
    fn new(inlets: usize) -> Self {
        Self {
            indices: Vec::with_capacity(inlets),
            positions: vec![usize::MAX; inlets],
        }
    }

    fn all(inlets: usize) -> Self {
        Self {
            indices: (0..inlets).collect(),
            positions: (0..inlets).collect(),
        }
    }

    fn insert(&mut self, index: usize) {
        if self.positions[index] == usize::MAX {
            self.positions[index] = self.indices.len();
            self.indices.push(index);
        }
    }

    fn remove(&mut self, index: usize) {
        let position = self.positions[index];
        if position == usize::MAX {
            return;
        }
        self.indices.swap_remove(position);
        if let Some(&moved) = self.indices.get(position) {
            self.positions[moved] = position;
        }
        self.positions[index] = usize::MAX;
    }

    fn pop(&mut self) -> Option<usize> {
        let index = *self.indices.last()?;
        self.remove(index);
        Some(index)
    }

    fn indices(&self) -> &[usize] {
        &self.indices
    }

    fn clear(&mut self) {
        for &index in &self.indices {
            self.positions[index] = usize::MAX;
        }
        self.indices.clear();
    }
}

/// What a source or stage may touch during one call: its pipeline's inlets, the bytes the
/// source asked for, and the session.
pub struct Cx<'a> {
    arena: &'a mut Arena,
    inlets: &'a [PortId],
    bytes: &'a mut VecDeque<(SegmentId, BufferHandle)>,
    reads: SmallVec<[SegmentId; 1]>,
    session: &'a VortexSession,
    exec: &'a mut ExecutionCtx,
    touched: &'a mut InletSet,
    changed: &'a mut InletSet,
}

impl Cx<'_> {
    /// Takes an inlet index whose sink appended data or closed since the last readiness
    /// check. Each index is reported once, and all inlets are reported on the first check.
    /// Remaining notifications are cleared after [`Source::ready`] returns. A source must
    /// account for its own consumption when caching inlet state.
    pub fn changed_inlet(&mut self) -> Option<usize> {
        self.changed.pop()
    }

    /// Queues a segment read during [`Source::ready`]. Return [`SourceReady::Read`] after
    /// queuing all the reads needed for this call.
    pub fn request_read(&mut self, segment: SegmentId) {
        self.reads.push(segment);
    }

    /// Whether a requested segment has arrived and can be taken.
    pub fn has_bytes(&self) -> bool {
        !self.bytes.is_empty()
    }

    /// The reading side of inlet `index` of this pipeline's source.
    pub fn inlet(&mut self, index: usize) -> Inlet<'_> {
        self.touched.insert(index);
        self.arena.inlet(self.inlets[index])
    }

    /// Takes an arrived segment's ID and bytes, oldest arrival first.
    pub fn take_segment(&mut self) -> Option<(SegmentId, BufferHandle)> {
        self.bytes.pop_front()
    }

    /// The session used for decoding and evaluation.
    pub fn session(&self) -> &VortexSession {
        self.session
    }

    /// The execution context for executing arrays.
    pub fn exec(&mut self) -> &mut ExecutionCtx {
        self.exec
    }
}

#[cfg(test)]
mod scheduling_tests;
