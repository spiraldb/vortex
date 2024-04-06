use vortex_error::{vortex_err, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::array::validity::ValidityArray;
use crate::compute::scalar_at;
use crate::{Array, WithArray};

pub trait ArrayValidity {
    fn is_valid(&self, index: usize) -> bool;
}

impl ArrayValidity for &dyn ValidityArray {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub enum ValidityMetadata {
    Valid,
    Invalid,
    Array,
}

impl<'v> From<&Validity<'v>> for ValidityMetadata {
    fn from(value: &Validity<'v>) -> Self {
        match value {
            Validity::Valid(_) => ValidityMetadata::Valid,
            Validity::Invalid(_) => ValidityMetadata::Invalid,
            Validity::Array(_) => ValidityMetadata::Array,
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

    pub fn try_from_validity_meta(
        meta: &ValidityMetadata,
        length: usize,
        array: Option<Array<'v>>,
    ) -> VortexResult<Validity<'v>> {
        match meta {
            ValidityMetadata::Valid => Ok(Validity::Valid(length)),
            ValidityMetadata::Invalid => Ok(Validity::Invalid(length)),
            ValidityMetadata::Array => array
                .map(|v| Validity::Array(v))
                .ok_or(vortex_err!("Expected validity array")),
        }
    }

    pub fn into_array(self) -> Option<Array<'v>> {
        match self {
            Validity::Array(a) => Some(a),
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
