use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;

use arrow_buffer::{ArrowNativeType, Buffer as ArrowBuffer, MutableBuffer};
use bytes::Bytes;
use itertools::Itertools;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use vortex_buffer::Buffer;
use vortex_dtype::{match_each_native_ptype, DType, NativePType, PType};
use vortex_error::{vortex_bail, VortexResult};

use crate::elementwise::{flat_array_iter, BinaryFn, UnaryFn};
use crate::iter::{Accessor, AccessorRef, Batch};
use crate::stats::StatsSet;
use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::variants::{ArrayVariants, PrimitiveArrayTrait};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoCanonical,
    TypedArray,
};

mod accessor;
mod compute;
mod stats;

impl_encoding!("vortex.primitive", 3u16, Primitive);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimitiveMetadata {
    validity: ValidityMetadata,
}

impl PrimitiveArray {
    pub fn new(buffer: Buffer, ptype: PType, validity: Validity) -> Self {
        let length = match_each_native_ptype!(ptype, |$P| {
            let (prefix, values, suffix) = unsafe { buffer.align_to::<$P>() };
            assert!(
                prefix.is_empty() && suffix.is_empty(),
                "buffer is not aligned"
            );
            values.len()
        });

        Self {
            typed: TypedArray::try_from_parts(
                DType::from(ptype).with_nullability(validity.nullability()),
                length,
                PrimitiveMetadata {
                    validity: validity.to_metadata(length).expect("invalid validity"),
                },
                Some(buffer),
                validity.into_array().into_iter().collect_vec().into(),
                StatsSet::new(),
            )
            .expect("should be valid"),
        }
    }

    pub fn from_vec<T: NativePType>(values: Vec<T>, validity: Validity) -> Self {
        match_each_native_ptype!(T::PTYPE, |$P| {
            PrimitiveArray::new(
                ArrowBuffer::from(MutableBuffer::from(unsafe { std::mem::transmute::<Vec<T>, Vec<$P>>(values) })).into(),
                T::PTYPE,
                validity,
            )
        })
    }

    pub fn from_nullable_vec<T: NativePType>(values: Vec<Option<T>>) -> Self {
        let elems: Vec<T> = values.iter().map(|v| v.unwrap_or_default()).collect();
        let validity = Validity::from(values.iter().map(|v| v.is_some()).collect::<Vec<_>>());
        Self::from_vec(elems, validity)
    }

    /// Creates a new array of type U8
    pub fn from_bytes(bytes: Bytes, validity: Validity) -> Self {
        let buffer = Buffer::from(bytes);

        PrimitiveArray::new(buffer, PType::U8, validity)
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(0, &Validity::DTYPE, self.len()))
    }

    pub fn ptype(&self) -> PType {
        // TODO(ngates): we can't really cache this anywhere?
        self.dtype().try_into().unwrap_or_else(|err| {
            panic!("Failed to convert dtype {} to ptype: {}", self.dtype(), err);
        })
    }

    pub fn buffer(&self) -> &Buffer {
        self.array().buffer().expect("missing buffer")
    }

    pub fn maybe_null_slice<T: NativePType>(&self) -> &[T] {
        assert_eq!(
            T::PTYPE,
            self.ptype(),
            "Attempted to get slice of type {} from array of type {}",
            T::PTYPE,
            self.ptype(),
        );

        let (prefix, values, suffix) = unsafe { self.buffer().as_ref().align_to::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        values
    }

    /// Convert the array into a mutable vec of the given type.
    /// If possible, this will be zero-copy.
    pub fn into_maybe_null_slice<T: NativePType + ArrowNativeType>(self) -> Vec<T> {
        assert_eq!(
            T::PTYPE,
            self.ptype(),
            "Attempted to get maybe_null_slice of type {} from array of type {}",
            T::PTYPE,
            self.ptype(),
        );
        self.into_buffer().into_vec::<T>().unwrap_or_else(|b| {
            let (prefix, values, suffix) = unsafe { b.as_ref().align_to::<T>() };
            assert!(prefix.is_empty() && suffix.is_empty());
            Vec::from(values)
        })
    }

    pub fn get_as_cast<T: NativePType>(&self, idx: usize) -> T {
        match_each_native_ptype!(self.ptype(), |$P| {
            T::from(self.maybe_null_slice::<$P>()[idx]).expect("failed to cast")
        })
    }

    pub fn reinterpret_cast(&self, ptype: PType) -> Self {
        if self.ptype() == ptype {
            return self.clone();
        }

        assert_eq!(
            self.ptype().byte_width(),
            ptype.byte_width(),
            "can't reinterpret cast between integers of two different widths"
        );

        PrimitiveArray::new(self.buffer().clone(), ptype, self.validity())
    }

    pub fn patch<P: AsPrimitive<usize>, T: NativePType + ArrowNativeType>(
        self,
        positions: &[P],
        values: &[T],
    ) -> VortexResult<Self> {
        if self.ptype() != T::PTYPE {
            vortex_bail!(MismatchedTypes: self.dtype(), T::PTYPE)
        }

        let validity = self.validity();

        let mut own_values = self.into_maybe_null_slice();
        // TODO(robert): Also patch validity
        for (idx, value) in positions.iter().zip_eq(values.iter()) {
            own_values[(*idx).as_()] = *value;
        }
        Ok(Self::from_vec(own_values, validity))
    }

    pub fn into_buffer(self) -> Buffer {
        self.into_array()
            .into_buffer()
            .expect("PrimitiveArray must have a buffer")
    }
}

impl ArrayTrait for PrimitiveArray {}

impl ArrayVariants for PrimitiveArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl<T: NativePType> Accessor<T> for PrimitiveArray {
    fn array_len(&self) -> usize {
        self.len()
    }

    fn is_valid(&self, index: usize) -> bool {
        ArrayValidity::is_valid(self, index)
    }

    fn array_validity(&self) -> Validity {
        self.validity()
    }

    #[inline]
    fn value_unchecked(&self, index: usize) -> T {
        self.maybe_null_slice::<T>()[index]
    }

    #[inline]
    fn decode_batch(&self, start_idx: usize) -> Vec<T> {
        let batch_size = <Self as Accessor<T>>::batch_size(self, start_idx);
        let mut v = Vec::<T>::with_capacity(batch_size);
        let null_slice = self.maybe_null_slice::<T>();

        unsafe {
            v.set_len(batch_size);
            ptr::copy_nonoverlapping(
                null_slice.as_ptr().add(start_idx),
                v.as_mut_ptr(),
                batch_size,
            );
        }

        v
    }
}

macro_rules! primitive_accessor_ref {
    ($self:expr, $ptype:ident) => {
        match $self.dtype() {
            DType::Primitive(PType::$ptype, _) => {
                let accessor = Arc::new($self.clone());
                Some(accessor)
            }
            _ => None,
        }
    };
}

impl PrimitiveArrayTrait for PrimitiveArray {
    fn f32_accessor(&self) -> Option<AccessorRef<f32>> {
        primitive_accessor_ref!(self, F32)
    }

    fn f64_accessor(&self) -> Option<AccessorRef<f64>> {
        primitive_accessor_ref!(self, F64)
    }

    fn u8_accessor(&self) -> Option<AccessorRef<u8>> {
        primitive_accessor_ref!(self, U8)
    }

    fn u16_accessor(&self) -> Option<AccessorRef<u16>> {
        primitive_accessor_ref!(self, U16)
    }

    fn u32_accessor(&self) -> Option<AccessorRef<u32>> {
        primitive_accessor_ref!(self, U32)
    }

    fn u64_accessor(&self) -> Option<AccessorRef<u64>> {
        primitive_accessor_ref!(self, U64)
    }

    fn i8_accessor(&self) -> Option<AccessorRef<i8>> {
        primitive_accessor_ref!(self, I8)
    }

    fn i16_accessor(&self) -> Option<AccessorRef<i16>> {
        primitive_accessor_ref!(self, I16)
    }

    fn i32_accessor(&self) -> Option<AccessorRef<i32>> {
        primitive_accessor_ref!(self, I32)
    }

    fn i64_accessor(&self) -> Option<AccessorRef<i64>> {
        primitive_accessor_ref!(self, I64)
    }
}

impl<T: NativePType> From<Vec<T>> for PrimitiveArray {
    fn from(values: Vec<T>) -> Self {
        Self::from_vec(values, Validity::NonNullable)
    }
}

impl<T: NativePType> IntoArray for Vec<T> {
    fn into_array(self) -> Array {
        PrimitiveArray::from(self).into_array()
    }
}

impl IntoCanonical for PrimitiveArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        Ok(Canonical::Primitive(self))
    }
}

impl ArrayValidity for PrimitiveArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for PrimitiveArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(&self.validity())
    }
}

impl Array {
    pub fn as_primitive(&self) -> PrimitiveArray {
        PrimitiveArray::try_from(self).expect("expected primitive array")
    }
}

impl UnaryFn for PrimitiveArray {
    fn unary<I: NativePType, O: NativePType, F: Fn(I) -> O>(
        &self,
        unary_fn: F,
    ) -> VortexResult<Array> {
        let mut output: Vec<MaybeUninit<O>> = Vec::with_capacity(self.len());
        unsafe { output.set_len(self.len()) };
        // let data = self.maybe_null_slice::<I>();

        for (index, item) in self.maybe_null_slice::<I>().iter().enumerate() {
            unsafe {
                *output.get_unchecked_mut(index) = MaybeUninit::new(unary_fn(*item));
            }
        }

        Ok(PrimitiveArray::from_vec(
            output
                .into_iter()
                .map(|o| unsafe { o.assume_init() })
                .collect(),
            self.validity(),
        )
        .into_array())
    }
}

impl BinaryFn for PrimitiveArray {
    fn binary<I: NativePType, U: NativePType, O: NativePType, F: Fn(I, U) -> O>(
        &self,
        rhs: Array,
        binary_fn: F,
    ) -> VortexResult<Array> {
        if !self.dtype().eq_ignore_nullability(rhs.dtype()) {
            vortex_bail!(MismatchedTypes: self.dtype(), rhs.dtype());
        }

        if PType::try_from(self.dtype())? != I::PTYPE {
            vortex_bail!(MismatchedTypes: self.dtype(), I::PTYPE);
        }

        let lhs = self.maybe_null_slice::<I>();
        let mut output: Vec<MaybeUninit<O>> = Vec::with_capacity(self.len());
        unsafe { output.set_len(self.len()) };

        let validity = self
            .validity()
            .and(rhs.with_dyn(|a| a.logical_validity().into_validity()))?;

        let rhs_iter = flat_array_iter::<U>(&rhs);
        let mut start_idx = 0;
        for batch in rhs_iter {
            let batch_len = batch.len();
            process_batch(
                &lhs[start_idx..start_idx + batch_len],
                batch,
                &binary_fn,
                start_idx,
                output.as_mut_slice(),
            );
            start_idx += batch_len;
        }

        Ok(PrimitiveArray::from_vec(
            output
                .into_iter()
                .map(|o| unsafe { o.assume_init() })
                .collect(),
            validity,
        )
        .into_array())
    }
}

fn process_batch<I: NativePType, U: NativePType, O: NativePType, F: Fn(I, U) -> O>(
    lhs: &[I],
    batch: Batch<U>,
    f: F,
    start_idx: usize,
    output: &mut [MaybeUninit<O>],
) {
    assert_eq!(batch.len(), lhs.len());

    if batch.len() == 1024 {
        let lhs: [I; 1024] = lhs.try_into().unwrap();
        let rhs: [U; 1024] = batch.data().try_into().unwrap();

        for idx in 0_usize..1024 {
            unsafe {
                *output.get_unchecked_mut(idx + start_idx) =
                    MaybeUninit::new(f(lhs[idx], rhs[idx]));
            }
        }
    } else {
        for (idx, rhs_item) in batch.data().iter().enumerate() {
            unsafe {
                *output.get_unchecked_mut(idx + start_idx) =
                    MaybeUninit::new(f(lhs[idx], *rhs_item));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use vortex_scalar::Scalar;

    use super::*;

    #[test]
    fn batched_iter() {
        let v = PrimitiveArray::from_vec((0_u32..10_000).collect(), Validity::AllValid);
        let iter = v.u32_iter().unwrap();

        let mut items_counter = 0;

        for batch in iter {
            let batch_size = batch.len();
            for idx in 0..batch_size {
                assert!(batch.is_valid(idx));
                assert_eq!((items_counter + idx) as u32, unsafe {
                    *batch.get_unchecked(idx)
                });
            }

            items_counter += batch_size;
        }
    }

    #[test]
    fn flattened_iter() {
        let v = PrimitiveArray::from_vec((0_u32..10_000).collect(), Validity::AllValid);
        let iter = v.u32_iter().unwrap();

        for (idx, v) in iter.flatten().enumerate() {
            assert_eq!(idx as u32, v.unwrap());
        }
    }

    #[test]
    fn binary_fn_example() {
        let input = PrimitiveArray::from_vec(vec![2u32, 2, 2, 2], Validity::AllValid);

        let scalar = Scalar::from(2u32);

        let o = input
            .unary(move |v: u32| {
                let scalar_v = u32::try_from(&scalar).unwrap();
                if v == scalar_v {
                    1_u8
                } else {
                    0_u8
                }
            })
            .unwrap();

        let output_iter = o
            .with_dyn(|a| a.as_primitive_array_unchecked().u8_iter())
            .unwrap()
            .flatten();

        for v in output_iter {
            assert_eq!(v.unwrap(), 1);
        }
    }
}
