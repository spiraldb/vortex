use arrow_buffer::Buffer;
use num_traits::PrimInt;
use std::fmt::{Debug, Formatter};

use crate::array::primitive::PrimitiveEncoding;
use crate::array::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::compute::flatten::{flatten_primitive, FlattenFn, FlattenedArray};
use crate::compute::ArrayCompute;
use crate::ptype::{NativePType, PType};
use crate::serde::vtable::{ComputeVTable, TakeFn, VTable};
use crate::serde::ArrayView;
use crate::validity::Validity;
use crate::{match_each_integer_ptype, match_each_native_ptype};
use vortex_error::{vortex_err, VortexResult};

pub struct PrimitiveView<'a> {
    ptype: PType,
    buffer: &'a Buffer,
    // Note(ngates): for other array views, children should be stored as &dyn Array.
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

impl<T: NativePType> Debug for dyn PrimitiveTrait<T> + '_ {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("primitive doooo")
    }
}

impl<'a, T: NativePType> PrimitiveTrait<T> for &ArrayView<'a> {
    fn ptype(&self) -> PType {
        todo!()
    }

    fn validity(&self) -> Option<Validity> {
        todo!()
    }

    fn typed_data(&self) -> &[T] {
        todo!()
    }

    fn to_array(&self) -> ArrayRef {
        todo!()
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
    fn take(&self, _array: &ArrayView<'view>, _indices: &dyn Array) -> VortexResult<ArrayRef> {
        todo!()
        // let pv = PrimitiveView::try_new(array)?;
        // TakeFn::take((&pv as &dyn PrimitiveTrait<u16>), )
        // match_each_native_ptype!(pv.ptype, |$T| {
        //     TakeFn::
        //     .take(indices)
        // })
    }
}

impl<T: NativePType> ArrayCompute for &dyn PrimitiveTrait<T> {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn crate::compute::take::TakeFn> {
        Some(self)
    }
}

impl<T: NativePType> FlattenFn for &dyn PrimitiveTrait<T> {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        todo!()
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
