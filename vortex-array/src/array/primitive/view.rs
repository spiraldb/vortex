use arrow_buffer::Buffer;
use num_traits::PrimInt;

use crate::array::validity::Validity;
use crate::array::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::compute::flatten::{flatten_primitive, FlattenFn, FlattenedArray};
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::match_each_integer_ptype;
use crate::ptype::{NativePType, PType};
use crate::serde::ArrayView;
use vortex_error::{vortex_err, VortexResult};

pub struct PrimitiveView<'a> {
    ptype: PType,
    buffer: &'a Buffer,
    // TODO(ngates): switch to ValidityView
    validity: Option<Validity>,
}

impl<'a> PrimitiveView<'a> {
    pub fn try_new(view: &'a ArrayView<'a>) -> VortexResult<Self> {
        // TODO(ngates): validate the number of buffers / children. We could even extract them?
        let ptype = PType::try_from(view.dtype())?;
        let buffer = view
            .buffers()
            .first()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Missing primitive buffer"))?;
        let validity = view
            .child(0, &Validity::DTYPE)
            // FIXME(ngates): avoid this clone.
            .map(|v| Validity::Array(Array::to_array(&v)));

        Ok(Self {
            ptype,
            buffer,
            validity,
        })
    }

    pub fn ptype(&self) -> PType {
        self.ptype
    }

    pub fn as_trait<T: NativePType>(&self) -> &dyn PrimitiveTrait<T> {
        assert_eq!(self.ptype, T::PTYPE);
        self
    }
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

// The question is how can we implement ArrayCompute for PrimitiveArray + PrimitiveView?
// We can't use a trait since typed_data doesn't work? Or maybe we can but we just return Buffer?
pub trait PrimitiveTrait<T: NativePType> {
    fn ptype(&self) -> PType;
    fn validity(&self) -> Option<Validity>;
    fn typed_data(&self) -> &[T];
    fn to_array(&self) -> ArrayRef;
}

// TODO(ngates): migrate all primitive compute over to PrimitiveTrait.
impl<T: NativePType> ArrayCompute for &dyn PrimitiveTrait<T> {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl<T: NativePType> FlattenFn for &dyn PrimitiveTrait<T> {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        todo!()
    }
}

impl<T: NativePType> TakeFn for &dyn PrimitiveTrait<T> {
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
