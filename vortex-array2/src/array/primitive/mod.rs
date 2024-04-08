mod compute;

use arrow_buffer::{Buffer, ScalarBuffer};
use serde::{Deserialize, Serialize};
use vortex::ptype::{NativePType, PType};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::impl_encoding;
use crate::validity::{ArrayValidity, Validity, ValidityMetadata};
use crate::ArrayMetadata;
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayView, ToArrayData};

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimitiveMetadata {
    ptype: PType,
    validity: ValidityMetadata,
}

pub struct PrimitiveArray<'a> {
    ptype: PType,
    dtype: &'a DType,
    buffer: &'a Buffer,
    validity: Validity<'a>,
}

impl PrimitiveArray<'_> {
    pub fn buffer(&self) -> &Buffer {
        self.buffer
    }

    pub fn validity(&self) -> &Validity {
        &self.validity
    }

    pub fn ptype(&self) -> PType {
        self.ptype
    }
}

impl<'a> TryFromArrayParts<'a, PrimitiveMetadata> for PrimitiveArray<'a> {
    fn try_from_parts(
        parts: &'a dyn ArrayParts<'a>,
        metadata: &'a PrimitiveMetadata,
    ) -> VortexResult<Self> {
        let buffer = parts.buffer(0).unwrap();
        Ok(PrimitiveArray {
            ptype: metadata.ptype,
            dtype: parts.dtype(),
            buffer,
            validity: metadata.validity.to_validity(parts.child(0, parts.dtype())),
        })
    }
}

impl PrimitiveData {
    fn try_new<T: NativePType>(buffer: ScalarBuffer<T>, validity: Validity) -> VortexResult<Self> {
        ArrayData::try_new(
            &PrimitiveEncoding,
            DType::from(T::PTYPE).with_nullability(validity.nullability()),
            Arc::new(PrimitiveMetadata {
                ptype: T::PTYPE,
                validity: validity.to_metadata(buffer.len() / T::PTYPE.byte_width())?,
            }),
            vec![buffer.into_inner()].into(),
            vec![validity.into_array_data()].into(),
        )
        .unwrap()
        .try_into()
    }
}

impl ArrayTrait for PrimitiveArray<'_> {
    fn dtype(&self) -> &DType {
        self.dtype
    }

    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }
}

impl ArrayValidity for PrimitiveArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }
}

impl ToArrayData for PrimitiveArray<'_> {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}
