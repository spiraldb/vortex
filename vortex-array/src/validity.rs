//! Array validity and nullability behavior, used by arrays and compute functions.

use std::fmt::{Debug, Display};
use std::ops::BitAnd;

use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer};
use serde::{Deserialize, Serialize};
use vortex_dtype::{DType, Nullability};
use vortex_error::{
    vortex_bail, vortex_err, vortex_panic, VortexError, VortexExpect as _, VortexResult,
};

use crate::array::BoolArray;
use crate::compute::unary::scalar_at_unchecked;
use crate::compute::{filter, slice, take};
use crate::stats::ArrayStatistics;
use crate::{Array, ArrayDType, IntoArray, IntoArrayVariant};

pub trait ArrayValidity {
    fn is_valid(&self, index: usize) -> bool;
    fn logical_validity(&self) -> LogicalValidity;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ValidityMetadata {
    NonNullable,
    AllValid,
    AllInvalid,
    Array,
}

impl Display for ValidityMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl ValidityMetadata {
    pub fn to_validity<F>(&self, array_fn: F) -> Validity
    where
        F: FnOnce() -> Array,
    {
        match self {
            Self::NonNullable => Validity::NonNullable,
            Self::AllValid => Validity::AllValid,
            Self::AllInvalid => Validity::AllInvalid,
            Self::Array => Validity::Array(array_fn()),
        }
    }
}

/// Validity information for an array
#[derive(Clone, Debug)]
pub enum Validity {
    /// Items *can't* be null
    NonNullable,
    /// All items are valid
    AllValid,
    /// All items are null
    AllInvalid,
    /// Specified items are null
    Array(Array),
}

impl Validity {
    /// The [`DType`] of the underlying validity array (if it exists).
    pub const DTYPE: DType = DType::Bool(Nullability::NonNullable);

    pub fn to_metadata(&self, length: usize) -> VortexResult<ValidityMetadata> {
        match self {
            Self::NonNullable => Ok(ValidityMetadata::NonNullable),
            Self::AllValid => Ok(ValidityMetadata::AllValid),
            Self::AllInvalid => Ok(ValidityMetadata::AllInvalid),
            Self::Array(a) => {
                // We force the caller to validate the length here.
                let validity_len = a.len();
                if validity_len != length {
                    vortex_bail!(
                        "Validity array length {} doesn't match array length {}",
                        validity_len,
                        length
                    )
                }
                Ok(ValidityMetadata::Array)
            }
        }
    }

    /// If Validity is [`Validity::Array`], returns the array, otherwise returns `None`.
    pub fn into_array(self) -> Option<Array> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    /// If Validity is [`Validity::Array`], returns a reference to the array array, otherwise returns `None`.
    pub fn as_array(&self) -> Option<&Array> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn nullability(&self) -> Nullability {
        match self {
            Self::NonNullable => Nullability::NonNullable,
            _ => Nullability::Nullable,
        }
    }

    /// Returns whether the `index` item is valid.
    #[inline]
    pub fn is_valid(&self, index: usize) -> bool {
        match self {
            Self::NonNullable | Self::AllValid => true,
            Self::AllInvalid => false,
            Self::Array(a) => {
                bool::try_from(&scalar_at_unchecked(a, index)).unwrap_or_else(|err| {
                    vortex_panic!(
                        err,
                        "Failed to get bool from Validity Array at index {}",
                        index
                    )
                })
            }
        }
    }

    #[inline]
    pub fn is_null(&self, index: usize) -> bool {
        !self.is_valid(index)
    }

    pub fn slice(&self, start: usize, stop: usize) -> VortexResult<Self> {
        match self {
            Self::Array(a) => Ok(Self::Array(slice(a, start, stop)?)),
            _ => Ok(self.clone()),
        }
    }

    pub fn take(&self, indices: &Array) -> VortexResult<Self> {
        match self {
            Self::NonNullable => Ok(Self::NonNullable),
            Self::AllValid => Ok(Self::AllValid),
            Self::AllInvalid => Ok(Self::AllInvalid),
            Self::Array(a) => Ok(Self::Array(take(a, indices)?)),
        }
    }

    pub fn filter(&self, predicate: &Array) -> VortexResult<Self> {
        match self {
            v @ (Validity::NonNullable | Validity::AllValid | Validity::AllInvalid) => {
                Ok(v.clone())
            }
            Validity::Array(arr) => Ok(Validity::Array(filter(arr, predicate)?)),
        }
    }

    pub fn to_logical(&self, length: usize) -> LogicalValidity {
        match self {
            Self::NonNullable => LogicalValidity::AllValid(length),
            Self::AllValid => LogicalValidity::AllValid(length),
            Self::AllInvalid => LogicalValidity::AllInvalid(length),
            Self::Array(a) => {
                // Logical validity should map into AllValid/AllInvalid where possible.
                if a.statistics().compute_min::<bool>().unwrap_or(false) {
                    LogicalValidity::AllValid(length)
                } else if a
                    .statistics()
                    .compute_max::<bool>()
                    .map(|m| !m)
                    .unwrap_or(false)
                {
                    LogicalValidity::AllInvalid(length)
                } else {
                    LogicalValidity::Array(a.clone())
                }
            }
        }
    }

    /// Logically & two Validity values of the same length
    pub fn and(self, rhs: Validity) -> VortexResult<Validity> {
        let validity = match (&self, &rhs) {
            // Should be pretty clear
            (Validity::NonNullable, Validity::NonNullable) => Validity::NonNullable,
            // Any `AllInvalid` makes the output all invalid values
            (Validity::AllInvalid, _) | (_, Validity::AllInvalid) => Validity::AllInvalid,
            // All truthy values on one side, which makes no effect on an `Array` variant
            (Validity::Array(a), Validity::AllValid)
            | (Validity::Array(a), Validity::NonNullable)
            | (Validity::NonNullable, Validity::Array(a))
            | (Validity::AllValid, Validity::Array(a)) => Validity::Array(a.clone()),
            // Both sides are all valid
            (Validity::NonNullable, Validity::AllValid)
            | (Validity::AllValid, Validity::NonNullable)
            | (Validity::AllValid, Validity::AllValid) => Validity::AllValid,
            // Here we actually have to do some work
            (Validity::Array(lhs), Validity::Array(rhs)) => {
                let lhs = BoolArray::try_from(lhs)?;
                let rhs = BoolArray::try_from(rhs)?;

                let lhs = lhs.boolean_buffer();
                let rhs = rhs.boolean_buffer();

                Validity::from(lhs.bitand(&rhs))
            }
        };

        Ok(validity)
    }

    /// Convert into a nullable variant
    pub fn into_nullable(self) -> Validity {
        match self {
            Self::NonNullable => Self::AllValid,
            _ => self,
        }
    }
}

impl PartialEq for Validity {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::NonNullable, Self::NonNullable) => true,
            (Self::AllValid, Self::AllValid) => true,
            (Self::AllInvalid, Self::AllInvalid) => true,
            (Self::Array(a), Self::Array(b)) => {
                let a_buffer = a
                    .clone()
                    .into_bool()
                    .vortex_expect("Failed to get Validity Array as BoolArray")
                    .boolean_buffer();
                let b_buffer = b
                    .clone()
                    .into_bool()
                    .vortex_expect("Failed to get Validity Array as BoolArray")
                    .boolean_buffer();
                a_buffer == b_buffer
            }
            _ => false,
        }
    }
}

impl From<Vec<bool>> for Validity {
    fn from(bools: Vec<bool>) -> Self {
        if bools.iter().all(|b| *b) {
            Self::AllValid
        } else if !bools.iter().any(|b| *b) {
            Self::AllInvalid
        } else {
            Self::Array(BoolArray::from(bools).into_array())
        }
    }
}

impl From<BooleanBuffer> for Validity {
    fn from(value: BooleanBuffer) -> Self {
        if value.count_set_bits() == value.len() {
            Self::AllValid
        } else if value.count_set_bits() == 0 {
            Self::AllInvalid
        } else {
            Self::Array(BoolArray::from(value).into_array())
        }
    }
}

impl From<NullBuffer> for Validity {
    fn from(value: NullBuffer) -> Self {
        value.into_inner().into()
    }
}

impl FromIterator<LogicalValidity> for Validity {
    fn from_iter<T: IntoIterator<Item = LogicalValidity>>(iter: T) -> Self {
        let validities: Vec<LogicalValidity> = iter.into_iter().collect();

        // If they're all valid, then return a single validity.
        if validities.iter().all(|v| v.all_valid()) {
            return Self::AllValid;
        }
        // If they're all invalid, then return a single invalidity.
        if validities.iter().all(|v| v.all_invalid()) {
            return Self::AllInvalid;
        }

        // Else, construct the boolean buffer
        let mut buffer = BooleanBufferBuilder::new(validities.iter().map(|v| v.len()).sum());
        for validity in validities {
            match validity {
                LogicalValidity::AllValid(count) => buffer.append_n(count, true),
                LogicalValidity::AllInvalid(count) => buffer.append_n(count, false),
                LogicalValidity::Array(array) => {
                    let array_buffer = array
                        .into_bool()
                        .vortex_expect("Failed to get Validity Array as BoolArray")
                        .boolean_buffer();
                    buffer.append_buffer(&array_buffer);
                }
            };
        }
        let bool_array = BoolArray::from(buffer.finish());
        Self::Array(bool_array.into_array())
    }
}

impl<'a, E> FromIterator<&'a Option<E>> for Validity {
    fn from_iter<T: IntoIterator<Item = &'a Option<E>>>(iter: T) -> Self {
        let bools: Vec<bool> = iter.into_iter().map(|option| option.is_some()).collect();
        Self::from(bools)
    }
}

#[derive(Clone, Debug)]
pub enum LogicalValidity {
    AllValid(usize),
    AllInvalid(usize),
    Array(Array),
}

impl LogicalValidity {
    pub fn try_new_from_array(array: Array) -> VortexResult<Self> {
        if !matches!(array.dtype(), &Validity::DTYPE) {
            vortex_bail!("Expected a non-nullable boolean array");
        }

        let true_count = array
            .statistics()
            .compute_true_count()
            .ok_or_else(|| vortex_err!("Failed to compute true count from validity array"))?;
        if true_count == array.len() {
            return Ok(Self::AllValid(array.len()));
        } else if true_count == 0 {
            return Ok(Self::AllInvalid(array.len()));
        }

        Ok(Self::Array(array))
    }

    pub fn to_null_buffer(&self) -> VortexResult<Option<NullBuffer>> {
        match self {
            Self::AllValid(_) => Ok(None),
            Self::AllInvalid(l) => Ok(Some(NullBuffer::new_null(*l))),
            Self::Array(a) => Ok(Some(NullBuffer::new(
                a.clone().into_bool()?.boolean_buffer(),
            ))),
        }
    }

    pub fn all_valid(&self) -> bool {
        matches!(self, Self::AllValid(_))
    }

    pub fn all_invalid(&self) -> bool {
        matches!(self, Self::AllInvalid(_))
    }

    pub fn len(&self) -> usize {
        match self {
            Self::AllValid(n) => *n,
            Self::AllInvalid(n) => *n,
            Self::Array(a) => a.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::AllValid(n) => *n == 0,
            Self::AllInvalid(n) => *n == 0,
            Self::Array(a) => a.is_empty(),
        }
    }

    pub fn into_validity(self) -> Validity {
        match self {
            Self::AllValid(_) => Validity::AllValid,
            Self::AllInvalid(_) => Validity::AllInvalid,
            Self::Array(a) => Validity::Array(a),
        }
    }
}

impl TryFrom<Array> for LogicalValidity {
    type Error = VortexError;

    fn try_from(array: Array) -> VortexResult<Self> {
        Self::try_new_from_array(array)
    }
}

impl IntoArray for LogicalValidity {
    fn into_array(self) -> Array {
        match self {
            Self::AllValid(len) => BoolArray::from(vec![true; len]).into_array(),
            Self::AllInvalid(len) => BoolArray::from(vec![false; len]).into_array(),
            Self::Array(a) => a,
        }
    }
}
