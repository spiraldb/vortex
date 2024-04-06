use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::array::validity::ValidityArray;
use crate::compute::scalar_at;
use crate::{Array, ArrayData, ToArrayData, WithArray};

pub trait ArrayValidity {
    fn is_valid(&self, index: usize) -> bool;
    // Maybe add to_bool_array() here?
}

impl ArrayValidity for &dyn ValidityArray {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub enum ValidityMetadata {
    NonNullable,
    Valid,
    Invalid,
    Array,
}

impl ValidityMetadata {
    pub fn try_from_validity(validity: Option<&Validity>, dtype: &DType) -> VortexResult<Self> {
        // We don't really need dtype for this conversion, but it's a good place to check
        // that the nullability and validity are consistent.
        match validity {
            None => {
                if dtype.nullability() != Nullability::NonNullable {
                    vortex_bail!("DType must be NonNullable if validity is absent")
                }
                Ok(ValidityMetadata::NonNullable)
            }
            Some(v) => {
                if dtype.nullability() != Nullability::Nullable {
                    vortex_bail!("DType must be Nullable if validity is present")
                }
                Ok(match v {
                    Validity::Valid(_) => ValidityMetadata::Valid,
                    Validity::Invalid(_) => ValidityMetadata::Invalid,
                    Validity::Array(_) => ValidityMetadata::Array,
                })
            }
        }
    }

    pub fn to_validity<'v>(&self, len: usize, array: Option<Array<'v>>) -> Option<Validity<'v>> {
        match self {
            ValidityMetadata::NonNullable => None,
            ValidityMetadata::Valid => Some(Validity::Valid(len)),
            ValidityMetadata::Invalid => Some(Validity::Invalid(len)),
            // TODO(ngates): should we return a result for this?
            ValidityMetadata::Array => Some(Validity::Array(array.unwrap())),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Validity<'v> {
    Valid(usize),
    Invalid(usize),
    Array(Array<'v>),
}

impl<'v> Validity<'v> {
    pub const DTYPE: DType = DType::Bool(Nullability::NonNullable);

    pub fn into_array_data(self) -> Option<ArrayData> {
        match self {
            Validity::Array(a) => Some(a.to_array_data()),
            _ => None,
        }
    }

    pub fn array(&self) -> Option<&Array> {
        match self {
            Validity::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Validity::Valid(l) => *l,
            Validity::Invalid(l) => *l,
            Validity::Array(a) => a.with_array(|a| a.len()),
        }
    }

    pub fn is_valid(&self, index: usize) -> bool {
        match self {
            Validity::Valid(_) => true,
            Validity::Invalid(_) => false,
            Validity::Array(a) => scalar_at(a, index).unwrap().try_into().unwrap(),
        }
    }
}
