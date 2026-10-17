// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Experimental strict row functions with statically dispatched host bindings.
//!
//! Functions select typed visitors. Hosts own decoding, validity, allocation, and publication.
//! Null inputs imply null output, and valid inputs cannot produce null. Callbacks must not panic
//! or have side effects other than writing their supplied row. Constants and retries can change
//! their evaluation count. This crate has no registration, serialization, or binary ABI contract.
//!
//! Start with [`RowFn`] and [`RowVisitor`] to define a function. [`plan`] selects its typed output
//! without decoding, and [`execute`] invokes it through host bindings. [`sink::ElementSink`] offers
//! safe initialized scalar rows for immediate errors. Uninitialized sinks require explicit unsafe
//! initialization evidence. See the package README for complete Arrow invocation and author guides.

#![deny(missing_docs)]

mod host;
pub use host::BatchBinding;
pub use host::Host;
pub use host::HostResult;
pub use host::Operand;
pub use host::Selection;
pub use host::TypeBinding;
pub use host::ValiditySummary;

mod input;
pub use input::FixedSizeList;
pub use input::InputBinding;
pub use input::RowKind;
pub use input::RowView;
pub use input::Utf8;

mod text;
pub use text::TextBinding;
pub use text::TextLayout;
pub use text::TextValue;

mod output;
pub use output::BooleanOutput;
pub use output::FailureEvidence;
pub use output::OutputBinding;
pub use output::OutputBuffer;

mod tuple;
pub use tuple::InputTuple;

mod prepare;
pub use prepare::PreparedRows;

pub mod sink;

mod visitor;
pub use visitor::RowFn;
pub use visitor::RowVisitor;

mod plan;
pub use plan::Plan;
pub use plan::plan;

mod execute;
pub use execute::execute;

pub use rowfn_kernels as kernels;
