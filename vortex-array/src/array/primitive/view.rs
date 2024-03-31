use arrow_buffer::Buffer;
use num_traits::PrimInt;

use crate::array::primitive::PrimitiveEncoding;
use crate::array::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::array2::{ArrayMetadata, ArrayView, ComputeVTable, TakeFn, VTable};
use crate::compute::flatten::flatten_primitive;
use crate::ptype::{NativePType, PType};
use crate::validity::Validity;
use crate::{match_each_integer_ptype, match_each_native_ptype};
use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

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

pub struct PrimitiveView<'a> {
    ptype: PType,
    buffer: &'a Buffer,
    validity: Option<Validity>,
}

impl<'a> PrimitiveView<'a> {
    pub fn try_new(view: &'a ArrayView<'a>) -> VortexResult<Self> {
        // TODO(ngates): validate the number of buffers / children. We could even extract them?
        let metadata = PrimitiveMetadata::try_from_bytes(view.metadata(), view.dtype())?;
        let buffer = view
            .buffers()
            .first()
            .ok_or_else(|| VortexError::InvalidSerde("Missing primitive buffer".into()))?;
        let validity = view
            .child(0, Validity::DTYPE)
            // FIXME(ngates): avoid this clone.
            .map(|v| Validity::Array(v.to_array()));

        Ok(Self {
            ptype: metadata.ptype,
            buffer,
            validity,
        })
    }

    pub fn as_dyn<T: NativePType>(&self) -> &dyn PrimitiveTrait<T> {
        assert_eq!(T::PTYPE, self.ptype);
        self
    }
}

// The question is how can we implement ArrayCompute for PrimitiveArray + PrimitiveView?
// We can't use a trait since typed_data doesn't work? Or maybe we can but we just return Buffer?
pub trait PrimitiveTrait<T: NativePType> {
    fn ptype(&self) -> PType;
    fn validity(&self) -> Option<Validity>;
    fn typed_data(&self) -> &[T];
    fn to_array(&self) -> ArrayRef;
}

impl<'a, T: NativePType> PrimitiveTrait<T> for PrimitiveView<'a> {
    fn ptype(&self) -> PType {
        self.ptype
    }

    fn validity(&self) -> Option<Validity> {
        self.validity.clone()
    }

    fn typed_data(&self) -> &[T] {
        self.buffer.typed_data::<T>()
    }

    fn to_array(&self) -> ArrayRef {
        PrimitiveArray::new(self.ptype, self.buffer.clone(), self.validity.clone()).into_array()
    }
}

impl<'view> VTable<ArrayView<'view>> for PrimitiveEncoding {
    fn len(&self, view: &ArrayView<'view>) -> usize {
        let p = PrimitiveView::try_new(view).unwrap();
        p.buffer.len() / p.ptype.byte_width()
    }

    fn to_array(&self, view: &ArrayView<'view>) -> ArrayRef {
        // TODO(ngates): seems silly to switch on PType for this?
        let pv = PrimitiveView::try_new(view).unwrap();
        match_each_native_ptype!(pv.ptype, |$T| {
            (&pv as &dyn PrimitiveTrait<$T>).to_array()
        })
    }

    fn compute(&self) -> &dyn ComputeVTable<ArrayView<'view>> {
        self
    }

    fn validate(&self, view: &ArrayView<'view>) -> VortexResult<()> {
        PrimitiveView::try_new(view).map(|_| ())
    }
}

impl<'view> ComputeVTable<ArrayView<'view>> for PrimitiveEncoding {
    fn take(&self) -> Option<&dyn TakeFn<ArrayView<'view>>> {
        Some(self)
    }
}

impl<'view> TakeFn<ArrayView<'view>> for PrimitiveEncoding {
    fn take(&self, array: &ArrayView<'view>, indices: &dyn Array) -> VortexResult<ArrayRef> {
        use crate::compute::take::TakeFn;
        let view = PrimitiveView::try_new(array)?;
        match_each_native_ptype!(view.ptype, |$T| {
            view.as_dyn::<$T>().take(indices)
        })
    }
}

impl<T: NativePType> crate::compute::take::TakeFn for &dyn PrimitiveTrait<T> {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let validity = self.validity().map(|v| v.take(indices)).transpose()?;
        let indices = flatten_primitive(indices)?;
        match_each_integer_ptype!(indices.ptype(), |$I| {
            Ok(PrimitiveArray::from_nullable(
                take_primitive(self.typed_data(), indices.typed_data::<$I>()),
                validity,
            ).into_array())
        })
    }
}

fn take_primitive<T: NativePType, I: NativePType + PrimInt>(array: &[T], indices: &[I]) -> Vec<T> {
    indices
        .iter()
        .map(|&idx| array[idx.to_usize().unwrap()])
        .collect()
}
