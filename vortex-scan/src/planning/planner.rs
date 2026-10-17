// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The planner side of the protocol: work scopes, object state, and planner outputs.

use std::fmt;
use std::ops::Range;

use futures::future::BoxFuture;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::request::IoBatch;
use vortex_io::request::IoConsumer;

use crate::planning::morsel::Morsel;

/// Logical position carried with work and output for future driver ordering policies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkScope {
    /// Ordinal of the file the work belongs to.
    pub file_ordinal: u64,
    /// File-local row domain covered by the work.
    pub rows: Range<u64>,
}

/// What an object needs next. Cheap and side-effect free.
///
/// `state()` is called before every `compute()`, and `compute()` only when `state()` is
/// `NeedsCompute`. It never lists requests: every request is published once, from `compute()`,
/// and the driver keeps each `Fetch` until it is delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    /// The object has finished and will be dropped without further calls.
    Done,
    /// The object can make progress on the CPU.
    NeedsCompute,
    /// The object waits for a fetch or asynchronous task it published. Any completion may make
    /// it runnable. Waiting without an outstanding fetch or task is a protocol error.
    Waiting,
}

/// An asynchronous completion owned by one planner or morsel.
///
/// The driver polls the future and calls the owner's `task_complete` method after success.
/// Errors fail the run. Cancelling the owner drops the future, so it must own any child task
/// handles whose execution should be cancelled with it.
pub struct WorkFuture {
    /// Identifies this task among the owner's outstanding tasks.
    pub id: u64,
    /// Waits for work whose result the owner retains through its own completion channel.
    pub future: BoxFuture<'static, VortexResult<()>>,
}

impl fmt::Debug for WorkFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkFuture")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// What one `compute()` produced.
///
/// Returning `NeedsIO` registers the batch immediately, and each request in it exactly once. The
/// next `state()` determines whether the object waits or has more CPU work. Optional requests
/// never require a wait or delivery. After `Done` the object is dropped without further calls.
pub enum PlannerOutput {
    /// The planner has finished.
    Done,
    /// A CPU-only checkpoint; the driver requeues the planner.
    Continue,
    /// Requests the planner publishes: fetches it needs, and optional hints.
    NeedsIO(IoBatch),
    /// A completion to await without blocking other work in the run.
    Await(WorkFuture),
    /// A child planner, with the scope it is responsible for.
    Planner(WorkScope, Box<dyn Planner>),
    /// A morsel authorised to produce arrays for the scope.
    Morsel(WorkScope, Box<dyn Morsel>),
}

/// A planner decides which work is needed.
///
/// It privately owns cursors, selections, and pending inputs. `compute()` advances CPU work
/// when `state()` reports `NeedsCompute` and may produce one child, one IO batch, a checkpoint,
/// or finish. A planner moves between threads with the run that owns it.
pub trait Planner: IoConsumer + Send {
    /// Reports what the planner needs next without doing work.
    fn state(&self) -> State;

    /// Advances CPU work. Only called when [`state`](Self::state) is `NeedsCompute`.
    fn compute(&mut self) -> VortexResult<PlannerOutput>;

    /// Receives successful completion of a task returned by [`PlannerOutput::Await`].
    fn task_complete(&mut self, _id: u64) -> VortexResult<()> {
        vortex_bail!("planner did not implement task completion")
    }
}

impl fmt::Debug for PlannerOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Done => f.write_str("Done"),
            Self::Continue => f.write_str("Continue"),
            Self::NeedsIO(batch) => f.debug_tuple("NeedsIO").field(batch).finish(),
            Self::Await(task) => f.debug_tuple("Await").field(task).finish(),
            Self::Planner(scope, _) => f.debug_tuple("Planner").field(scope).finish(),
            Self::Morsel(scope, _) => f.debug_tuple("Morsel").field(scope).finish(),
        }
    }
}
