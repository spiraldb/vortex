// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![deny(missing_docs)]

//! A morsel-driven push executor for Vortex layouts.
//!
//! Each worker owns an arena and drives one contiguous row range, or morsel. The immutable
//! [`ExecPlan`] compiles operators into physical pipelines. Execution activates sources with
//! authoritative row selections; sources produce [`PushBatch`] values and pass them through
//! downstream stages inline. Multi-input operators align batches at pipeline boundaries.
//!
//! [`ExecNode::next_plan`] names I/O before execution. [`ExecNode::push_start`] activates a
//! source, [`ExecNode::push_input`] accepts an upstream batch, and [`ExecNode::push_end`] closes
//! an input. [`ExecNode::push_resume`] continues a stage after an exact dependency is ready;
//! [`ExecNode::push_credit`] returns downstream capacity to a producer. Operators retain state
//! across these calls, and [`ExecNode::retire`] releases it when the morsel finishes.
//!
//! The executor never polls storage futures. [`PushCx::ready`] can try a source-provided
//! non-blocking read; a miss suspends the pipeline on its exact ticket. Reads leave the scan
//! through [`MorselScan::take_io`] and are answered by [`IoCompletions`], for example through
//! [`SegmentSourceDriver`]. Authoritative [`ActivationTarget`] decisions control which sources
//! execute; optional [`DemandTarget`] hints only affect I/O scheduling.
//!
//! [`MorselScan::into_stream`] exposes ordered output with bounded capacity and cancellation.
//! [`MorselScan::run`] collects that output. Raw request cells deduplicate segment reads across
//! the scan. Leased [`cells::SharedCells`] retain decoded chunks until the last overlapping
//! morsel retires; decoded sharing can be disabled independently.
//!
//! Flat, chunked, and non-nullable struct layouts are supported, together with filter and
//! conjunction operators. Zoned and legacy-statistics wrappers are transparent. Unsupported
//! layouts fail during [`build_plan`]. This experimental crate is not part of the public API.

pub mod build;
pub mod cells;
pub mod driver;
pub mod executor;
#[cfg(any(test, feature = "_test-harness"))]
pub mod fixtures;
#[cfg(any(test, feature = "_test-harness"))]
pub mod harness;
pub mod io;
pub mod node;
pub mod nodes;
pub mod source;
pub mod stats;
#[cfg(any(test, feature = "_test-harness"))]
pub mod tpch;
#[cfg(any(test, feature = "_test-harness"))]
pub mod workloads;

pub use build::ExecPlan;
pub use build::SourceActivation;
pub use build::SourceRole;
pub use build::build_plan;
pub use driver::DemandHintDelivery;
pub use driver::MorselScan;
pub use driver::MorselStream;
pub use driver::morsels;
pub use executor::PushMorselScanExecutor;
pub use io::IoCompletions;
pub use io::IoDemand;
pub use io::IoDemandStream;
pub use io::IoKey;
pub use io::IoPriority;
pub use io::IoRequest;
pub use io::NowaitProbe;
pub use node::ActivationRows;
pub use node::ActivationTarget;
pub use node::DemandTarget;
pub use node::ExecNode;
pub use node::InputPort;
pub use node::NodeState;
pub use node::PlanCx;
pub use node::PlanPoll;
pub use node::PushBatch;
pub use node::PushCx;
pub use node::Route;
pub use node::Value;
pub use source::SegmentSourceDriver;
pub use stats::ScanStats;

#[cfg(test)]
mod tests;
