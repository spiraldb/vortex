use std::sync::Arc;

use arrow_buffer::BooleanBuffer;
use itertools::Itertools;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array::bool::{BoolData, BoolMetadata};
use crate::validity::Validity;

impl BoolData {
    pub fn try_new(buffer: BooleanBuffer, validity: Validity) -> VortexResult<Self> {
        Ok(Self::new_unchecked(
            DType::Bool(validity.nullability()),
            Arc::new(BoolMetadata {
                validity: validity.to_metadata(buffer.len())?,
                length: buffer.len(),
            }),
            vec![buffer.into_inner()].into(),
            validity.to_array_data().into_iter().collect_vec().into(),
        ))
    }

    pub fn from_vec(bools: Vec<bool>, validity: Validity) -> Self {
        let buffer = BooleanBuffer::from(bools);
        Self::try_new(buffer, validity).unwrap()
    }
}

impl From<BooleanBuffer> for BoolData {
    fn from(value: BooleanBuffer) -> Self {
        BoolData::try_new(value, Validity::NonNullable).unwrap()
    }
}

impl From<Vec<bool>> for BoolData {
    fn from(value: Vec<bool>) -> Self {
        BoolData::from_vec(value, Validity::NonNullable)
    }
}

impl FromIterator<Option<bool>> for BoolData {
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

        BoolData::try_new(BooleanBuffer::from(values), Validity::from(validity)).unwrap()
    }
}
