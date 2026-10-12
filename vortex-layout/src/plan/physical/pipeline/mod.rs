// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Push-based execution of physical plans as pipelines.
//!
//! A [`Scan`] runs a [`PlanRef`](crate::plan::physical::PlanRef) over a list of splits, row ranges
//! of the plan's domain. Each split is compiled into pipelines: a pipeline is one
//! [`Source`](super::operator::Source), zero or more [`Operator`](super::operator::Operator)s,
//! and the ports its last stage writes. The driver asks the source for a batch and pushes it
//! through the stages into the ports. Every port runs from one pipeline's sink,
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

pub use self::compile::Chain;
pub use self::compile::Compiler;
pub use self::compile::Reach;
pub(super) use self::port::Arena;
pub use self::port::Inlet;
pub(super) use self::port::PortId;
pub use self::scan::ReadId;
pub use self::scan::ReadRequest;
pub use self::scan::Scan;
pub use self::scan::Split;
pub use self::scan::Turn;

/// Batches an inlet holds before its sink is blocked, unless the plan joining it says
/// otherwise; see [`Compiler::join_with`].
pub const DEFAULT_CAPACITY: usize = 8;

#[cfg(test)]
mod scheduling_tests;
