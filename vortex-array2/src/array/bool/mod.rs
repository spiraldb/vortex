mod compute;

use arrow_buffer::{BooleanBuffer, Buffer};
use serde::{Deserialize, Serialize};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::impl_encoding;
use crate::validity::Validity;
use crate::validity::{ArrayValidity, ValidityMetadata};
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
    validity: Option<Validity<'a>>,
    // TODO(ngates): unpack metadata?
    metadata: &'a BoolMetadata,
    // TODO(ngates): we support statistics by reference to a dyn trait.
    //  This trait is implemented for ArrayView and ArrayData and is passed into here as part
    //  of ArrayParts.
    //  e.g. stats: &dyn Statistics,
}

impl BoolArray<'_> {
    pub fn buffer(&self) -> &Buffer {
        self.buffer
    }

    pub fn validity(&self) -> Option<&Validity> {
        self.validity.as_ref()
    }

    pub fn metadata(&self) -> &BoolMetadata {
        self.metadata
    }

    pub fn boolean_buffer(&self) -> BooleanBuffer {
        BooleanBuffer::new(self.buffer.clone(), 0, self.metadata.length)
    }
}

impl<'v> TryFromArrayParts<'v, BoolMetadata> for BoolArray<'v> {
    fn try_from_parts(
        parts: &'v dyn ArrayParts<'v>,
        metadata: &'v BoolMetadata,
    ) -> VortexResult<Self> {
        Ok(BoolArray {
            dtype: parts.dtype(),
            buffer: parts
                .buffer(0)
                .ok_or(vortex_err!("BoolArray requires a buffer"))?,
            validity: metadata
                .validity
                .to_validity(metadata.length, parts.child(0, &Validity::DTYPE)),
            metadata,
        })
    }
}

impl BoolData {
    pub fn try_new(buffer: BooleanBuffer, validity: Option<Validity>) -> VortexResult<Self> {
        if let Some(v) = &validity {
            assert_eq!(v.len(), buffer.len());
        }
        let dtype = DType::Bool(validity.is_some().into());
        let metadata = BoolMetadata {
            validity: ValidityMetadata::try_from_validity(validity.as_ref(), &dtype)?,
            length: buffer.len(),
        };
        let validity_array = validity.and_then(|v| v.into_array_data());
        Ok(Self::new_unchecked(
            dtype,
            Arc::new(metadata),
            vec![buffer.into_inner()].into(),
            vec![validity_array].into(),
        ))
    }
}

impl ArrayTrait for BoolArray<'_> {
    fn dtype(&self) -> &DType {
        self.dtype
    }

    fn len(&self) -> usize {
        self.metadata().length
    }
}

impl ArrayValidity for BoolArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().map(|v| v.is_valid(index)).unwrap_or(true)
    }
}

impl ToArrayData for BoolArray<'_> {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::bool::BoolData;
    use crate::compute::scalar_at;
    use crate::IntoArray;

    #[test]
    fn bool_array() {
        let arr = BoolData::try_new(vec![true, false, true].into(), None)
            .unwrap()
            .into_array();

        let scalar: bool = scalar_at(&arr, 0).unwrap().try_into().unwrap();
        assert!(scalar);
    }
}
