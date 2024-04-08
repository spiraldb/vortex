mod compute;

use arrow_buffer::{BooleanBuffer, Buffer};
use serde::{Deserialize, Serialize};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::impl_encoding;
use crate::validity::Validity;
use crate::validity::{ArrayValidity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayMetadata;
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
    // TODO(ngates): we support statistics by reference to a dyn trait.
    //  This trait is implemented for ArrayView and ArrayData and is passed into here as part
    //  of ArrayParts.
    //  e.g. stats: &dyn Statistics,
}

impl BoolArray<'_> {
    pub fn buffer(&self) -> &Buffer {
        self.buffer
    }

    pub fn validity(&self) -> &Validity {
        &self.validity
    }

    pub fn boolean_buffer(&self) -> BooleanBuffer {
        BooleanBuffer::new(self.buffer.clone(), 0, self.length)
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
            vec![validity.into_array_data()].into(),
        ))
    }

    pub fn from_vec(bools: Vec<bool>) -> Self {
        let buffer = BooleanBuffer::from(bools);
        Self::try_new(buffer, Validity::NonNullable).unwrap()
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
}

impl ToArrayData for BoolArray<'_> {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}

impl AcceptArrayVisitor for BoolArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(self.validity())
    }
}

#[cfg(test)]
mod tests {
    use crate::array::bool::BoolData;
    use crate::compute::scalar_at;
    use crate::IntoArray;

    #[test]
    fn bool_array() {
        let arr = BoolData::from_vec(vec![true, false, true]).into_array();
        let scalar: bool = scalar_at(&arr, 0).unwrap().try_into().unwrap();
        assert!(scalar);
    }
}
