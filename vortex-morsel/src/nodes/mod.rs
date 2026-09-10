// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The operator state machines: one per plan operator the executor drives (segment scan,
//! concat, pack, take, eval, filter), plus the executor's own mask producers (conjunct, demand)
//! and its project root.

mod chunked;
mod conjunct;
mod demand;
mod dict;
mod eval;
mod filter;
mod flat;
mod project;
mod struct_;

pub use chunked::ChunkedExec;
pub use conjunct::ConjunctExec;
pub use conjunct::ConjunctSlot;
pub use demand::DemandExec;
pub use dict::DictExec;
pub use eval::EvalExec;
pub use filter::FilterExec;
pub use flat::FlatExec;
pub use project::ProjectExec;
pub use struct_::StructExec;

/// The mask density at or above which a predicate is evaluated over the whole range and
/// intersected afterwards, rather than over the selected rows only.
///
/// Mirrors `EXPR_EVAL_THRESHOLD` in the V1 flat reader so the two executors make the same
/// regime choice on the same data.
pub(crate) const EXPR_EVAL_THRESHOLD: f64 = 0.2;
