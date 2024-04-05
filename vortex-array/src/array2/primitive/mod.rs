mod compute;

use arrow_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array2::ArrayValidity;
use crate::array2::TypedArrayView;
use crate::array2::{ArrayData, TypedArrayData};
use crate::array2::{ArrayEncoding, ArrayMetadata, TryFromArrayMetadata};
use crate::array2::{ArrayView, ToArrayData};
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
    fn dtype(&self) -> &DType;
    fn ptype(&self) -> PType;
    fn buffer(&self) -> &Buffer;
}

impl PrimitiveData {
    pub fn from_vec<T: NativePType>(values: Vec<T>) -> Self {
        ArrayData::try_new(
            &PrimitiveEncoding,
            DType::from(T::PTYPE),
            Arc::new(PrimitiveMetadata(T::PTYPE)),
            vec![Buffer::from_vec(values)].into(),
            vec![].into(),
        )
        .unwrap()
        .try_into()
        .unwrap()
    }
}

impl PrimitiveArray for PrimitiveData {
    fn dtype(&self) -> &DType {
        self.data().dtype()
    }

    fn ptype(&self) -> PType {
        self.metadata().ptype()
    }

    fn buffer(&self) -> &Buffer {
        self.data().buffers().first().unwrap()
    }
}

impl PrimitiveArray for PrimitiveView<'_> {
    fn dtype(&self) -> &DType {
        self.view().dtype()
    }

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

impl TryFromArrayMetadata for PrimitiveMetadata {
    fn try_from_metadata(_metadata: Option<&[u8]>) -> VortexResult<Self> {
        todo!()
    }
}

impl TryFromArrayView for PrimitiveView<'_> {
    fn try_from_view(view: &ArrayView) -> VortexResult<Self> {
        todo!()
    }
}

impl TryFromArrayData for PrimitiveData {
    fn try_from_data(data: &ArrayData) -> VortexResult<Self> {
        todo!()
    }
}

impl ArrayTrait for &dyn PrimitiveArray {
    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }
}

impl ArrayValidity for &dyn PrimitiveArray {
    fn is_valid(&self, index: usize) -> bool {
        todo!()
    }
}

impl ToArrayData for &dyn PrimitiveArray {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}
