mod compute;

use arrow_buffer::Buffer;
use vortex::ptype::{NativePType, PType};
use vortex_error::VortexResult;
use vortex_schema::{DType, Nullability};

use crate::array::validity::Validity;
use crate::compute::scalar_at;
use crate::impl_encoding;
use crate::validity::ArrayValidity;
use crate::{Array, IntoArray};
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayMetadata, TryFromArrayMetadata};
use crate::{ArrayView, ToArrayData};
use crate::{ToArray, TypedArrayView};

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
    fn validity(&self) -> Option<Array>;
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

    fn validity(&self) -> Option<Array> {
        match self.dtype().nullability() {
            Nullability::NonNullable => None,
            Nullability::Nullable => Some(self.data().child(0).unwrap().to_array()),
        }
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

    fn validity(&self) -> Option<Array> {
        match self.dtype().nullability() {
            Nullability::NonNullable => None,
            Nullability::Nullable => {
                Some(self.view().child(0, &Validity::DTYPE).unwrap().into_array())
            }
        }
    }
}

impl TryFromArrayMetadata for PrimitiveMetadata {
    fn try_from_metadata(_metadata: Option<&[u8]>) -> VortexResult<Self> {
        todo!()
    }
}

impl<'v> TryFromArrayView<'v> for PrimitiveView<'v> {
    fn try_from_view(view: &'v ArrayView<'v>) -> VortexResult<Self> {
        // TODO(ngates): validate the view.
        Ok(PrimitiveView::new_unchecked(
            view.clone(),
            PrimitiveMetadata::try_from_metadata(view.metadata())?,
        ))
    }
}

impl TryFromArrayData for PrimitiveData {
    fn try_from_data(data: &ArrayData) -> VortexResult<Self> {
        // TODO(ngates): validate the array data.
        Ok(Self::from_data_unchecked(data.clone()))
    }
}

impl ArrayTrait for &dyn PrimitiveArray {
    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }
}

impl ArrayValidity for &dyn PrimitiveArray {
    fn is_valid(&self, index: usize) -> bool {
        if let Some(v) = self.validity() {
            scalar_at(&v, index).unwrap().try_into().unwrap()
        } else {
            true
        }
    }
}

impl ToArrayData for &dyn PrimitiveArray {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}
