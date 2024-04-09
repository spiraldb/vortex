mod compute;
mod stats;

use arrow_buffer::{BooleanBuffer, Buffer};
use serde::{Deserialize, Serialize};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::stats::Statistics;
use crate::validity::{ArrayValidity, ValidityMetadata};
use crate::validity::{LogicalValidity, Validity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayMetadata;
use crate::{impl_encoding, IntoArrayData};
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayView, ToArrayData};

impl_encoding!("vortex.bool", Bool);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BoolMetadata {
    validity: ValidityMetadata,
    length: usize,
}

pub struct BoolArray<'a> {
    dtype: &'a DType,
    buffer: &'a Buffer,
    validity: Validity<'a>,
    length: usize,
    statistics: &'a dyn Statistics,
}

impl BoolArray<'_> {
    pub fn buffer(&self) -> BooleanBuffer {
        // TODO(ngates): look into whether we should store this on BoolArray
        BooleanBuffer::new(self.buffer.clone(), 0, self.length)
    }

    pub fn validity(&self) -> &Validity {
        &self.validity
    }
}

impl<'v> TryFromArrayParts<'v, BoolMetadata> for BoolArray<'v> {
    fn try_from_parts(parts: &'v dyn ArrayParts, metadata: &'v BoolMetadata) -> VortexResult<Self> {
        Ok(BoolArray {
            dtype: parts.dtype(),
            // FIXME(ngates): implement our own BooleanBuffer that doesn't take ownership of the bytes
            buffer: parts
                .buffer(0)
                .ok_or(vortex_err!("BoolArray requires a buffer"))?,
            validity: metadata
                .validity
                .to_validity(parts.child(0, &Validity::DTYPE)),
            length: metadata.length,
            statistics: parts.statistics(),
        })
    }
}

impl BoolData {
    pub fn try_new(buffer: BooleanBuffer, validity: Validity) -> VortexResult<Self> {
        Ok(Self::new_unchecked(
            DType::Bool(validity.nullability()),
            Arc::new(BoolMetadata {
                validity: validity.to_metadata(buffer.len())?,
                length: buffer.len(),
            }),
            vec![buffer.into_inner()].into(),
            vec![validity.to_array_data_data()].into(),
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

impl ArrayTrait for BoolArray<'_> {
    fn dtype(&self) -> &DType {
        self.dtype
    }

    fn len(&self) -> usize {
        self.length
    }
}

impl ArrayValidity for BoolArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity.to_logical(self.len())
    }
}

impl ToArrayData for BoolArray<'_> {
    fn to_array_data(&self) -> ArrayData {
        BoolData::try_new(self.buffer().clone(), self.validity().clone())
            .unwrap()
            .into_array_data()
    }
}

impl AcceptArrayVisitor for BoolArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer().inner())?;
        visitor.visit_validity(self.validity())
    }
}

#[cfg(test)]
mod tests {
    use crate::array::bool::BoolData;
    use crate::compute::scalar_at::scalar_at;
    use crate::IntoArray;

    #[test]
    fn bool_array() {
        let arr = BoolData::from(vec![true, false, true]).into_array();
        let scalar: bool = scalar_at(&arr, 0).unwrap().try_into().unwrap();
        assert!(scalar);
    }
}
