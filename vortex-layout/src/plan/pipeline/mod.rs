// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Push-based execution of physical plans as pipelines.
//!
//! A [`Scan`] runs a [`PlanRef`](crate::plan::PlanRef) over a list of splits, row ranges of the
//! plan's domain. Each split is compiled into pipelines: a pipeline is one [`Source`], zero or
//! more [`Operator`]s, and the ports its last stage writes. The driver asks the source for a batch
//! and pushes it through the stages into the ports. Every port runs from one pipeline's sink,
//! the end that pushes into its outlets, to another's source, which reads it as an inlet, and
//! that source declares how many batches it may hold.
//!
//! Only a source blocks, and only on a segment read it requested or an inlet it waits on, which
//! it reports as a [`Blocked`]; a pipeline whose outlet is full is held back by the driver before
//! its source runs. The scheduler re-runs a pipeline when the one condition it waits on clears.
//!
//! The owner of a scan answers reads: [`Scan::step`] returns each read as a [`Turn::Read`], and
//! [`Scan::deliver`] hands its bytes back. Nothing in a scan awaits.
//!
//! # Masks
//!
//! Every mask a scan applies is known when the pipelines that apply it are compiled: the split's
//! selection, and in a [`Query`](crate::plan::Query) the rows the earlier conjuncts kept. The
//! compiler places the mask in the stage that reads the rows, so chunks a mask selects nothing
//! of are never built and never read.
//!
//! # Sharing
//!
//! A segment several readers decode is decoded once. Before the scan starts, one pass over the
//! plans every split's stages run records the rows each segment is read for, so the splits that
//! will read it are found by binary search over the splits. The first reader of a segment read
//! more than once builds one pipeline that decodes it and fans the decoded array out into one
//! single-use port per reader, tagged with the reader's split: a reader in the same stage, a
//! later stage of a query, or a later split claims its own port. A split's ports it never
//! claims, because its rows were pruned or a stage never ran, leave the sharing pipeline's
//! outlets and are freed when it finishes, so nothing is ever pushed into a port nobody will
//! read, and a claimed port is dropped once its source has drained it, so a decoded segment
//! lives only as long as a split that may still read it.

mod compile;
pub(crate) mod ops;
mod port;
mod query;
mod scan;
mod stream;
pub mod synthetic;

use std::collections::VecDeque;

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexResult;
use vortex_session::VortexSession;

pub use self::compile::Chain;
pub use self::compile::Compiler;
pub use self::compile::Reach;
pub use self::compile::Shared;
pub(crate) use self::compile::overlapping;
use self::port::Arena;
pub use self::port::Inlet;
use self::port::PortId;
pub use self::scan::ReadId;
pub use self::scan::ReadRequest;
pub use self::scan::Scan;
pub use self::scan::Split;
pub use self::scan::Turn;
pub use self::stream::ScanStream;
pub use self::stream::execute;
use crate::segments::SegmentId;

/// Batches an inlet holds before its sink is blocked, unless the plan joining it says
/// otherwise; see [`Compiler::join_with`].
pub const DEFAULT_CAPACITY: usize = 8;

/// What a source says back when asked for its next batch.
#[derive(Debug)]
pub enum Step {
    /// A batch for the stage above. The source is asked again at once.
    Batch(ArrayRef),
    /// A segment to read. The source is asked again at once, so it may have several reads in
    /// flight; the bytes come through [`Cx::take_segment`] in the order they arrive, and while
    /// none has, the source answers [`Blocked::Io`].
    Read(SegmentId),
    /// No batch until the condition clears. The source is not asked again before it does.
    Blocked(Blocked),
    /// No batch will ever come again. The source is not asked again.
    Finished,
}

/// What a source waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Blocked {
    /// A read it asked for has not arrived.
    Io,
    /// Inlet `inlet` is open and has room, so its sink can still push into it or close it, either
    /// of which wakes the source. It may already hold batches, as when a source waits for it to
    /// fill. Waiting on a closed or full inlet waits forever; debug builds catch it.
    Inlet(usize),
}

/// The start of a pipeline: produces batches from its inlets and the segments it asks to have
/// read.
pub trait Source: Send {
    /// What the source has ready: a batch, a segment to read, why it has neither, or that it
    /// has finished.
    fn ready(&mut self, cx: &mut Cx<'_>) -> VortexResult<Step>;
}

/// What an operator is handed by the stage below it.
#[derive(Debug)]
pub enum Input {
    /// The next batch.
    Batch(ArrayRef),
    /// The stage below has finished: nothing more comes. Handed over once, last.
    End,
}

/// A stage of a pipeline after its source: transforms what the stage below hands it, one input
/// at a time, and never blocks.
pub trait Operator: Send {
    /// Computes what to push on from `input`, if anything. At [`Input::End`] it returns whatever
    /// it still holds; it is not called again after.
    fn compute(&mut self, input: Input, cx: &mut Cx<'_>) -> VortexResult<Option<ArrayRef>>;
}

/// What a source or stage may touch during one call: its pipeline's inlets, the bytes the
/// source asked for, and the session.
pub struct Cx<'a> {
    arena: &'a mut Arena,
    inlets: &'a [PortId],
    bytes: &'a mut VecDeque<(SegmentId, BufferHandle)>,
    session: &'a VortexSession,
    exec: &'a mut ExecutionCtx,
    /// The inlets read during this run, each once, so only their sinks are checked for room
    /// after it, and whether each inlet is listed.
    touched: &'a mut Vec<usize>,
    listed: &'a mut [bool],
}

impl Cx<'_> {
    /// The reading side of inlet `index` of this pipeline's source.
    pub fn inlet(&mut self, index: usize) -> Inlet<'_> {
        if !self.listed[index] {
            self.listed[index] = true;
            self.touched.push(index);
        }
        self.arena.inlet(self.inlets[index])
    }

    /// A segment the source asked for that has arrived, oldest arrival first, with its bytes.
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
#[cfg(test)]
mod tests;
