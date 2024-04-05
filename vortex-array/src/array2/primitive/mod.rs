mod compute;

use arrow_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array2::ArrayCompute;
use crate::array2::ArrayView;
use crate::array2::TypedArrayView;
use crate::array2::{ArrayData, TypedArrayData};
use crate::array2::{ArrayEncoding, ArrayMetadata, FromArrayMetadata};
use crate::impl_encoding;
use crate::ptype::{NativePType, PType};

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone, Debug)]
pub struct PrimitiveMetadata(PType);
impl PrimitiveMetadata {
    pub fn ptype(&self) -> PType {
        self.0
    }
}

pub trait PrimitiveArray {
    fn ptype(&self) -> PType;
    fn buffer(&self) -> &Buffer;
}

impl PrimitiveData {
    pub fn from_vec<T: NativePType>(values: Vec<T>) -> Self {
        ArrayData::new(
            &PrimitiveEncoding,
            DType::from(T::PTYPE),
            Arc::new(PrimitiveMetadata(T::PTYPE)),
            vec![Buffer::from_vec(values)].into(),
            vec![].into(),
        )
        .as_typed()
    }
}

impl PrimitiveArray for PrimitiveData {
    fn ptype(&self) -> PType {
        self.metadata().ptype()
    }

    fn buffer(&self) -> &Buffer {
        self.data()
            .buffers()
            .first()
            // This assertion is made by construction.
            .expect("PrimitiveArray must have a single buffer")
    }
}

impl PrimitiveArray for PrimitiveView<'_> {
    fn ptype(&self) -> PType {
        self.metadata().ptype()
    }

    fn buffer(&self) -> &Buffer {
        self.view()
            .buffers()
            .first()
            .expect("PrimitiveView must have a single buffer")
    }
}

impl FromArrayMetadata for PrimitiveMetadata {
    fn try_from(_metadata: Option<&[u8]>) -> VortexResult<Self> {
        todo!()
    }
}

impl FromArrayView for PrimitiveView<'_> {
    fn try_from(view: &ArrayView) -> VortexResult<Self> {
        todo!()
    }
}

impl FromArrayData for PrimitiveData {
    fn try_from(data: &ArrayData) -> VortexResult<Self> {
        todo!()
    }
}
