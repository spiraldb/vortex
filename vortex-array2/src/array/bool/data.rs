use std::collections::HashMap;

use arrow_buffer::BooleanBuffer;
use itertools::Itertools;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::bool::{BoolArray, BoolMetadata, OwnedBoolArray};
use crate::buffer::Buffer;
use crate::validity::Validity;

impl BoolArray<'_> {
    pub fn try_new(buffer: BooleanBuffer, validity: Validity) -> VortexResult<Self> {
        Self::try_from_parts(
            DType::Bool(validity.nullability()),
            BoolMetadata {
                validity: validity.to_metadata(buffer.len())?,
                length: buffer.len(),
            },
            vec![Buffer::Owned(buffer.into_inner())].into(),
            validity.to_array_data().into_iter().collect_vec().into(),
            HashMap::default(),
        )
    }

    pub fn from_vec(bools: Vec<bool>, validity: Validity) -> Self {
        let buffer = BooleanBuffer::from(bools);
        Self::try_new(buffer, validity).unwrap()
    }
}

impl From<BooleanBuffer> for OwnedBoolArray {
    fn from(value: BooleanBuffer) -> Self {
        BoolArray::try_new(value, Validity::NonNullable).unwrap()
    }
}

impl From<Vec<bool>> for OwnedBoolArray {
    fn from(value: Vec<bool>) -> Self {
        BoolArray::from_vec(value, Validity::NonNullable)
    }
}

impl FromIterator<Option<bool>> for OwnedBoolArray {
    fn from_iter<I: IntoIterator<Item = Option<bool>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity: Vec<bool> = Vec::with_capacity(lower);
        let values: Vec<bool> = iter
            .map(|i| {
                validity.push(i.is_some());
                i.unwrap_or_default()
            })
            .collect::<Vec<_>>();

        BoolArray::try_new(BooleanBuffer::from(values), Validity::from(validity)).unwrap()
    }
}
