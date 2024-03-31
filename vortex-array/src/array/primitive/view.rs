use arrow_buffer::{Buffer, ScalarBuffer};

use vortex_error::VortexResult;
use vortex_schema::{DType, Nullability};

use crate::array::primitive::PrimitiveEncoding;
use crate::array::{Array, ArrayRef};
use crate::array2::{ArrayData, ArrayMetadata, ArrayView, TypedArrayData, TypedArrayView, VTable};
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::ptype::{NativePType, PType};

#[allow(dead_code)]
#[derive(Debug)]
pub struct PrimitiveMetadata {
    dtype: DType,
    ptype: PType,
}

impl PrimitiveMetadata {
    pub fn new(ptype: PType) -> Self {
        Self {
            dtype: DType::from(ptype),
            ptype,
        }
    }
}

impl ArrayMetadata for PrimitiveMetadata {
    fn to_bytes(&self) -> Option<Vec<u8>> {
        None
    }

    fn try_from_bytes(_bytes: Option<&[u8]>, dtype: &DType) -> VortexResult<Self> {
        let ptype = PType::try_from(dtype)?;
        Ok(PrimitiveMetadata {
            dtype: dtype.clone(),
            ptype,
        })
    }
}

pub type PrimitiveView<'a> = TypedArrayView<'a, PrimitiveMetadata>;

/// The "owned" version of a PrimitiveArray.
/// Not all arrays have to be implemented using TypedArrayData, but it can short-cut a lot of
/// implementation details. This should not preclude implementing the Array and Encoding traits
/// directly.
///
/// Maybe we make a downcast_impl that takes an expression used to downcast an array and then
/// re-invoke the function on it. For example,
///      downcast_impl!(ArrayView, { view.as_typed::<T>() });
#[allow(dead_code)]
pub type PrimitiveData = TypedArrayData<PrimitiveMetadata>;

impl<T: NativePType> ArrayCompute for &dyn PrimitiveTrait<T> {
    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl<T: NativePType> TakeFn for &dyn PrimitiveTrait<T> {
    fn take(&self, _indices: &dyn Array) -> VortexResult<ArrayRef> {
        todo!()
    }
}

impl<T: NativePType> From<Vec<T>> for PrimitiveData {
    fn from(value: Vec<T>) -> Self {
        PrimitiveData::new(
            &PrimitiveEncoding,
            DType::from(T::PTYPE),
            PrimitiveMetadata::new(T::PTYPE),
            Vec::new(),
            vec![ScalarBuffer::from(value).into_inner()],
        )
    }
}

// The question is how can we implement ArrayCompute for PrimitiveArray + PrimitiveView?
// We can't use a trait since typed_data doesn't work? Or maybe we can but we just return Buffer?
pub trait PrimitiveTrait<T: NativePType> {
    fn ptype(&self) -> PType;
    fn typed_data(&self) -> &[T];
}

impl<'a, T: NativePType> PrimitiveTrait<T> for PrimitiveView<'a> {
    fn ptype(&self) -> PType {
        self.ptype()
    }

    fn typed_data(&self) -> &[T] {
        self.buffer().typed_data::<T>()
    }
}

impl VTable<ArrayData> for PrimitiveEncoding {
    fn len(&self, _array: &ArrayData) -> usize {
        todo!()
    }

    fn validate(&self, _array: &ArrayData) -> VortexResult<()> {
        todo!()
    }
}

impl<'view> VTable<ArrayView<'view>> for PrimitiveEncoding {
    fn len(&self, view: &ArrayView<'view>) -> usize {
        view.try_as_typed::<PrimitiveMetadata>().unwrap().len()
    }

    fn validate(&self, view: &ArrayView<'view>) -> VortexResult<()> {
        view.try_as_typed::<PrimitiveMetadata>().map(|_| ())
    }
}

impl PrimitiveView<'_> {
    pub fn ptype(&self) -> PType {
        self.metadata().ptype
    }

    pub fn nullability(&self) -> Nullability {
        self.metadata().dtype.nullability()
    }

    pub fn buffer(&self) -> &Buffer {
        self.view().buffers().first().expect("Missing buffer")
    }
}
