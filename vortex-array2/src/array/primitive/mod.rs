mod stats;

use arrow_buffer::{ArrowNativeType, Buffer, ScalarBuffer};
use serde::{Deserialize, Serialize};
use vortex::ptype::{NativePType, PType};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::compute::ArrayCompute;
use crate::impl_encoding;
use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayMetadata;
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayView, ToArrayData};

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimitiveMetadata {
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

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        self.buffer.typed_data::<T>()
    }
}

impl<'a> TryFromArrayParts<'a, PrimitiveMetadata> for PrimitiveArray<'a> {
    fn try_from_parts(
        parts: &'a dyn ArrayParts,
        metadata: &'a PrimitiveMetadata,
    ) -> VortexResult<Self> {
        let buffer = parts.buffer(0).unwrap();
        let ptype: PType = parts.dtype().try_into()?;
        Ok(PrimitiveArray {
            ptype,
            dtype: parts.dtype(),
            buffer,
            validity: metadata.validity.to_validity(parts.child(0, parts.dtype())),
        })
    }
}

impl PrimitiveData {
    pub fn try_new<T: NativePType + ArrowNativeType>(
        buffer: ScalarBuffer<T>,
        validity: Validity,
    ) -> VortexResult<Self> {
        Ok(Self::new_unchecked(
            DType::from(T::PTYPE).with_nullability(validity.nullability()),
            Arc::new(PrimitiveMetadata {
                validity: validity.to_metadata(buffer.len() / T::PTYPE.byte_width())?,
            }),
            vec![buffer.into_inner()].into(),
            vec![validity.to_array_data_data()].into(),
        ))
    }

    pub fn from_vec<T: NativePType + ArrowNativeType>(values: Vec<T>, validity: Validity) -> Self {
        Self::try_new(ScalarBuffer::from(values), validity).unwrap()
    }

    pub fn from_nullable_vec<T: NativePType + ArrowNativeType>(values: Vec<Option<T>>) -> Self {
        let elems: Vec<_> = values.iter().map(|v| v.unwrap_or_default()).collect();
        let validity = Validity::from(values.iter().map(|v| v.is_some()).collect::<Vec<_>>());
        Self::from_vec(elems, validity)
    }
}

impl<T: NativePType> From<Vec<T>> for PrimitiveData {
    fn from(values: Vec<T>) -> Self {
        PrimitiveData::from_vec(values, Validity::NonNullable)
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

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl ToArrayData for PrimitiveArray<'_> {
    fn to_array_data(&self) -> ArrayData {
        ArrayData::try_new(
            &PrimitiveEncoding,
            self.dtype().clone(),
            Arc::new(PrimitiveMetadata {
                validity: self.validity().to_metadata(self.len()).unwrap(),
            }),
            vec![self.buffer().clone()].into(),
            vec![].into(),
        )
        .unwrap()
    }
}

impl AcceptArrayVisitor for PrimitiveArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(self.validity())
    }
}

impl ArrayCompute for PrimitiveArray<'_> {}
