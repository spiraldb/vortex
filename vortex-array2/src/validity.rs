use arrow_buffer::{BooleanBuffer, NullBuffer};
use serde::{Deserialize, Serialize};
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::array::bool::BoolData;
use crate::compute::flatten::flatten_bool;
use crate::compute::scalar_at::scalar_at;
use crate::compute::take::take;
use crate::{Array, ArrayData, ToArray, ToArrayData, WithArray};

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

impl ValidityMetadata {
    pub fn to_validity<'v>(&self, array: Option<Array<'v>>) -> Validity<'v> {
        match self {
            ValidityMetadata::NonNullable => Validity::NonNullable,
            ValidityMetadata::AllValid => Validity::AllValid,
            ValidityMetadata::AllInvalid => Validity::AllInvalid,
            // TODO(ngates): should we return a result for this?
            ValidityMetadata::Array => Validity::Array(array.unwrap()),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Validity<'v> {
    NonNullable,
    AllValid,
    AllInvalid,
    Array(Array<'v>),
}

impl<'v> Validity<'v> {
    pub const DTYPE: DType = DType::Bool(Nullability::NonNullable);

    pub fn to_array_data_data(self) -> Option<ArrayData> {
        match self {
            Validity::Array(a) => Some(a.to_array_data()),
            _ => None,
        }
    }

    pub fn to_metadata(&self, length: usize) -> VortexResult<ValidityMetadata> {
        match self {
            Validity::NonNullable => Ok(ValidityMetadata::NonNullable),
            Validity::AllValid => Ok(ValidityMetadata::AllValid),
            Validity::AllInvalid => Ok(ValidityMetadata::AllInvalid),
            Validity::Array(a) => {
                // We force the caller to validate the length here.
                let validity_len = a.with_array(|a| a.len());
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

    pub fn array(&self) -> Option<&Array> {
        match self {
            Validity::Array(a) => Some(a),
            _ => None,
        }
    }

    pub fn nullability(&self) -> Nullability {
        match self {
            Validity::NonNullable => Nullability::NonNullable,
            _ => Nullability::Nullable,
        }
    }

    pub fn is_valid(&self, index: usize) -> bool {
        match self {
            Validity::NonNullable | Validity::AllValid => true,
            Validity::AllInvalid => false,
            Validity::Array(a) => scalar_at(a, index).unwrap().try_into().unwrap(),
        }
    }

    pub fn take(&self, indices: &Array) -> VortexResult<Validity> {
        match self {
            Validity::NonNullable => Ok(Validity::NonNullable),
            Validity::AllValid => Ok(Validity::AllValid),
            Validity::AllInvalid => Ok(Validity::AllInvalid),
            Validity::Array(a) => Ok(Validity::Array(take(a, indices)?)),
        }
    }

    pub fn to_logical(&self, length: usize) -> LogicalValidity {
        match self {
            Validity::NonNullable => LogicalValidity::AllValid(length),
            Validity::AllValid => LogicalValidity::AllValid(length),
            Validity::AllInvalid => LogicalValidity::AllInvalid(length),
            Validity::Array(a) => LogicalValidity::Array(a.to_array_data()),
        }
    }
}

impl From<Vec<bool>> for Validity<'static> {
    fn from(bools: Vec<bool>) -> Self {
        if bools.iter().all(|b| *b) {
            Validity::AllValid
        } else if !bools.iter().any(|b| *b) {
            Validity::AllInvalid
        } else {
            Validity::Array(BoolData::from_vec(bools, Validity::NonNullable).to_array_data())
        }
    }
}

impl<'a> FromIterator<Validity<'a>> for Validity<'static> {
    fn from_iter<T: IntoIterator<Item = Validity<'a>>>(iter: T) -> Self {
        todo!()
    }
}

impl<'a, E> FromIterator<&'a Option<E>> for Validity<'static> {
    fn from_iter<T: IntoIterator<Item = &'a Option<E>>>(iter: T) -> Self {
        let bools: Vec<bool> = iter.into_iter().map(|option| option.is_some()).collect();
        Validity::from(bools)
    }
}

#[derive(Clone, Debug)]
pub enum LogicalValidity {
    AllValid(usize),
    AllInvalid(usize),
    Array(ArrayData),
}

impl LogicalValidity {
    pub fn to_null_buffer(&self) -> VortexResult<Option<NullBuffer>> {
        match self {
            LogicalValidity::AllValid(_) => Ok(None),
            LogicalValidity::AllInvalid(l) => Ok(Some(NullBuffer::new_null(*l))),
            LogicalValidity::Array(a) => {
                let bool_data = flatten_bool(&a.to_array())?;
                Ok(Some(NullBuffer::new(bool_data.as_ref().buffer())))
            }
        }
    }
}
