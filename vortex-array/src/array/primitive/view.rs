use crate::array::primitive::PrimitiveEncoding;
use crate::array::{Array, ArrayRef};
use crate::array2::{ArrayData, ArrayMetadata, ArrayView, TypedArrayData, TypedArrayView, VTable};
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::ptype::{NativePType, PType};
use crate::stats::Stats;
use crate::validity::{ArrayValidity, Validity};
use arrow_buffer::{Buffer, ScalarBuffer};
use std::any::Any;
use std::sync::Arc;
use vortex_error::VortexResult;
use vortex_schema::IntWidth::_32;
use vortex_schema::Nullability::Nullable;
use vortex_schema::Signedness::Signed;
use vortex_schema::{DType, Nullability};

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

pub type PrimitiveView<'a> = TypedArrayView<'a, PrimitiveMetadata>;

/// The "owned" version of a PrimitiveArray.
/// Not all arrays have to be implemented using TypedArrayData, but it can short-cut a lot of
/// implementation details. This should not preclude implementing the Array and Encoding traits
/// directly.
#[allow(dead_code)]
pub type PrimitiveData = TypedArrayData<PrimitiveMetadata>;

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

// Need some trait for primitive arrays?

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

impl<'a> ArrayCompute for PrimitiveView<'a> {}

impl<'a> ArrayValidity for PrimitiveView<'a> {
    fn validity(&self) -> Option<Validity> {
        todo!()
    }
}

impl<'a> ArrayDisplay for PrimitiveView<'a> {
    fn fmt(&self, _fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}

impl<'a> Array for PrimitiveView<'a> {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        todo!()
    }

    fn to_array(&self) -> ArrayRef {
        todo!()
    }

    fn into_array(self) -> ArrayRef {
        todo!()
    }

    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn dtype(&self) -> &DType {
        &DType::Int(_32, Signed, Nullable)
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        todo!()
    }

    fn encoding(&self) -> EncodingRef {
        todo!()
    }

    fn nbytes(&self) -> usize {
        todo!()
    }
}
