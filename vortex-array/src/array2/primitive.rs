use arrow_buffer::Buffer;
use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

use crate::array2::data::{ArrayData, TypedArrayData};
use crate::array2::view::{ArrayChildren, TypedArrayView};
use crate::array2::{ArrayEncoding, ArrayMetadata, ParseArrayMetadata};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::ptype::{NativePType, PType};
use crate::scalar::Scalar;
use crate::serde::ArrayView;
use crate::{impl_encoding, match_each_native_ptype};

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone)]
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

impl ArrayCompute for &dyn PrimitiveArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}
impl ScalarAtFn for &dyn PrimitiveArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match_each_native_ptype!(self.ptype(), |$T| {
            Ok(Scalar::from(self.buffer().typed_data::<$T>()[index]))
        })
    }
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

impl ArrayChildren for PrimitiveView<'_> {
    fn child_array_data(&self) -> Vec<ArrayData> {
        todo!()
    }
}

impl ParseArrayMetadata for PrimitiveMetadata {
    fn try_from(_metadata: Option<&[u8]>) -> VortexResult<Self> {
        todo!()
    }
}

impl<'v> TryFrom<Option<&'v [u8]>> for PrimitiveMetadata {
    type Error = VortexError;

    fn try_from(_value: Option<&'v [u8]>) -> Result<Self, Self::Error> {
        todo!()
    }
}
