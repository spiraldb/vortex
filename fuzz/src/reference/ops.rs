// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Structural operations and their plain Rust reference implementations.

use std::ops::Range;

use arbitrary::Unstructured;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_error::VortexResult;
use vortex_mask::Mask;

use super::value::Value;

/// An operation applied to both the encoded array and the reference values.
#[derive(Debug, Clone)]
pub enum RefAction {
    Slice(Range<usize>),
    Take(Vec<Option<usize>>),
    Filter(Vec<bool>),
    ScalarAt(Vec<usize>),
}

/// The outcome of an action: either a new array to keep operating on, or a list of scalars.
pub enum Outcome<T> {
    Array(T),
    Scalars(Vec<Value>),
}

impl RefAction {
    pub fn arbitrary(u: &mut Unstructured<'_>, len: usize) -> arbitrary::Result<Self> {
        Ok(match u.int_in_range(0..=3)? {
            0 => {
                let start = u.int_in_range(0..=len)?;
                let end = u.int_in_range(start..=len)?;
                RefAction::Slice(start..end)
            }
            1 if len > 0 => {
                let n = u.int_in_range(0..=len.min(64))?;
                RefAction::Take(
                    (0..n)
                        .map(|_| {
                            Ok(if u.ratio(1, 8)? {
                                None
                            } else {
                                Some(u.choose_index(len)?)
                            })
                        })
                        .collect::<arbitrary::Result<_>>()?,
                )
            }
            2 => RefAction::Filter(
                (0..len)
                    .map(|_| u.arbitrary())
                    .collect::<arbitrary::Result<_>>()?,
            ),
            _ if len > 0 => {
                let n = u.int_in_range(1..=4)?;
                RefAction::ScalarAt(
                    (0..n)
                        .map(|_| u.choose_index(len))
                        .collect::<arbitrary::Result<_>>()?,
                )
            }
            _ => RefAction::Slice(0..0),
        })
    }

    /// Apply the action with the plain Rust reference.
    pub fn reference(&self, values: &[Value]) -> Outcome<Vec<Value>> {
        match self {
            RefAction::Slice(range) => Outcome::Array(values[range.clone()].to_vec()),
            RefAction::Take(indices) => Outcome::Array(
                indices
                    .iter()
                    .map(|i| i.map_or(Value::Null, |i| values[i].clone()))
                    .collect(),
            ),
            RefAction::Filter(mask) => Outcome::Array(
                values
                    .iter()
                    .zip(mask)
                    .filter(|(_, keep)| **keep)
                    .map(|(v, _)| v.clone())
                    .collect(),
            ),
            RefAction::ScalarAt(indices) => {
                Outcome::Scalars(indices.iter().map(|i| values[*i].clone()).collect())
            }
        }
    }

    /// Apply the action to a Vortex array.
    pub fn vortex(
        &self,
        array: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Outcome<ArrayRef>> {
        Ok(match self {
            RefAction::Slice(range) => Outcome::Array(array.slice(range.clone())?),
            RefAction::Take(indices) => {
                let indices = if indices.iter().all(Option::is_some) {
                    PrimitiveArray::from_iter(indices.iter().flatten().map(|i| *i as u64))
                        .into_array()
                } else {
                    PrimitiveArray::from_option_iter(indices.iter().map(|i| i.map(|i| i as u64)))
                        .into_array()
                };
                Outcome::Array(array.take(indices)?)
            }
            RefAction::Filter(mask) => {
                Outcome::Array(array.filter(Mask::from_iter(mask.iter().copied()))?)
            }
            RefAction::ScalarAt(indices) => Outcome::Scalars(
                indices
                    .iter()
                    .map(|i| {
                        array
                            .execute_scalar(*i, ctx)
                            .map(|s| super::from_scalar(&s))
                    })
                    .collect::<VortexResult<_>>()?,
            ),
        })
    }
}
