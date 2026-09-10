// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![deny(missing_docs)]

//! An experimental morsel-driven scan executor for Vortex layouts.
//!
//! This crate is the P1 spine of the design recorded in
//! `docs/developer-guide/internals/scan-execution-models/morsel-based-plan-execution.md`: the scan
//! is cut into *morsels* (contiguous root row ranges), and each morsel is driven by a tree of
//! [`Operator`] state machines that own their children and pull values from them.
//!
//! The three calls of the contract are:
//!
//! * [`Operator::look_ahead`] — planning. An operator *names* the IO it will need by registering
//!   [`IoUse`](io::IoUse)s through the context, which hands back tickets. Operators do not read
//!   during look-ahead. Look-ahead can block: an operator that needs a read before it can name more
//!   pushes the ticket into the context, returns [`LookAhead::Blocked`], keeps its cursor, and is
//!   polled again when a dependency settles. Parents visit all independent children before blocking.
//! * [`Operator::next`] — value production. When a named required cell is still unissued,
//!   [`Cx::ready`] may attempt one caller-provided probe guaranteed not to wait on storage (Linux
//!   files use `preadv2(RWF_NOWAIT)`). A hit is consumed inline. A miss hands the read out as
//!   required demand and the operator parks on the exact ticket; the owning worker resumes the
//!   same morsel once it is completed.
//! * [`Operator::close`] — retirement. Every lease the morsel held is released exactly once.
//!
//! Compared to the V1 `LayoutReader` path this executor differs in two measurable ways:
//!
//! 1. There is no async task per evaluation. Planning and execution stay on the affinity-owned
//!    worker, which parks while its exact reads are outstanding; a blocked worker cannot execute
//!    another morsel.
//! 2. Each worker owns one operator tree and one active morsel. Trees never migrate, and
//!    emission order is restored by morsel index.
//!
//! The executor never touches storage. Reads it wants started leave a [`MorselScan`] as
//! [`IoDemand`](io::IoDemand) on a stream taken with [`MorselScan::take_io`], and whoever owns
//! storage answers them through [`IoCompletions`](io::IoCompletions); a blocked worker parks on
//! its exact cells until they are completed. [`SegmentSourceDriver`] serves that demand from any
//! [`SegmentSource`](vortex_layout::segments::SegmentSource) as one task on the caller's runtime.
//!
//! Raw request cells are shared for the lifetime of a scan, deduplicating both pending and
//! completed segment reads. Decoded chunks use leased shared cells ([`cells::SharedCells`]): a
//! decoded chunk lives exactly while some not-yet-retired morsel holds a lease computed from the
//! morsel cut, and is dropped at the last release. Decoded sharing can be disabled independently
//! as a differential-test and benchmark mode.
//!
//! The morsel's selection mask is produced once, by the root's mask child, and buffered in the
//! context ([`MaskBuffer`]) for every filter in the body to read with its own cursor. Operators own their children; live demand views share masks with registered I/O uses.
//!
//! Only the FLAT, CHUNKED, STRUCT and DICT layout nodes are supported, plus the FILTER, EVAL and
//! CONJUNCT operators. Anything else is rejected at build time by [`build::build_plan`].

pub mod build;
pub mod cells;
pub mod demand;
pub mod driver;
mod executor;
#[cfg(any(test, feature = "_test-harness"))]
pub mod fixtures;
#[cfg(any(test, feature = "_test-harness"))]
pub mod harness;
pub mod io;
#[cfg(any(test, feature = "_test-harness"))]
pub mod io_trace;
pub mod node;
pub mod nodes;
pub mod source;
pub mod stats;
pub mod tee;
#[cfg(any(test, feature = "_test-harness"))]
pub mod tpch;
#[cfg(any(test, feature = "_test-harness"))]
pub mod workloads;

pub use build::ExecPlan;
pub use build::build_plan;
pub use build::build_plan_for_ranges;
pub use build::natural_morsels_for;
pub use demand::DemandRef;
pub use demand::RowDomain;
pub use driver::MorselExecutor;
pub use driver::MorselScan;
pub use driver::ScanCancellation;
pub use driver::morsels;
pub use executor::MorselScanExecutor;
pub use io::IoCompletions;
pub use io::IoDemand;
pub use io::IoDemandStream;
pub use io::IoKey;
pub use io::IoPriority;
pub use io::IoRequest;
pub use io::NowaitProbe;
pub use node::Batch;
pub use node::Child;
pub use node::Cx;
pub use node::LookAhead;
pub use node::MorselRows;
pub use node::Operator;
pub use node::Step;
pub use node::Tree;
pub use node::Value;
pub use source::IoAnswerer;
pub use source::SegmentSourceDriver;
pub use stats::MorselTrace;
pub use stats::ScanStats;
pub use tee::MaskBuffer;

#[cfg(test)]
mod tests;
