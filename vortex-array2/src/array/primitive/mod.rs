mod compute;

use arrow_buffer::Buffer;
use vortex::ptype::{NativePType, PType};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::impl_encoding;
use crate::validity::{ArrayValidity, Validity, ValidityMetadata};
use crate::ArrayMetadata;
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayView, ToArrayData};

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone, Debug)]
pub struct PrimitiveMetadata {
    ptype: PType,
    validity: ValidityMetadata,
}

impl TryParseArrayMetadata for PrimitiveMetadata {
    fn try_parse_metadata(_metadata: Option<&[u8]>) -> VortexResult<Self> {
        todo!()
    }
}

pub struct PrimitiveArray<'a> {
    ptype: PType,
    dtype: &'a DType,
    buffer: &'a Buffer,
    validity: Option<Validity<'a>>,
}

impl PrimitiveArray<'_> {
    pub fn buffer(&self) -> &Buffer {
        self.buffer
    }

    pub fn validity(&self) -> Option<&Validity> {
        self.validity.as_ref()
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
        let length = buffer.len() / metadata.ptype.byte_width();
        Ok(PrimitiveArray {
            ptype: metadata.ptype,
            dtype: parts.dtype(),
            buffer,
            validity: metadata
                .validity
                .to_validity(length, parts.child(0, parts.dtype())),
        })
    }
}

impl PrimitiveData {
    pub fn from_vec<T: NativePType>(values: Vec<T>) -> Self {
        ArrayData::try_new(
            &PrimitiveEncoding,
            DType::from(T::PTYPE),
            Arc::new(PrimitiveMetadata {
                ptype: T::PTYPE,
                validity: ValidityMetadata::NonNullable,
            }),
            vec![Buffer::from_vec(values)].into(),
            vec![].into(),
        )
        .unwrap()
        .try_into()
        .unwrap()
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
        self.validity().map(|v| v.is_valid(index)).unwrap_or(true)
    }
}

impl ToArrayData for PrimitiveArray<'_> {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}
