// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Matches between stored aggregate partials and requested statistics.

use crate::expr::Expression;

/// An expression that resolves a requested statistic from a stored aggregate partial.
///
/// Each expression has the requested aggregate's partial-state dtype. Its outer nullability may
/// be widened to nullable for unavailable statistics; nested fields retain their declared types.
/// Expressions must preserve the requested partial's representation of empty and unknown states.
/// Approximate expressions must preserve the requested bound's direction, and consumers must use
/// them only in proofs that remain valid for a conservative bound.
#[derive(Clone, Debug)]
pub enum StatMatch {
    /// The expression provides the requested partial exactly.
    Exact(Expression),
    /// The expression provides a conservative bound in the requested partial representation.
    Approximate(Expression),
}
