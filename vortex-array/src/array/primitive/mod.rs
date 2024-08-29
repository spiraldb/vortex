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

use crate::iter::{Accessor, AccessorRef};
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

impl PrimitiveArrayTrait for PrimitiveArray {
    fn f32_accessor(&self) -> Option<AccessorRef<f32>> {
        match self.dtype() {
            DType::Primitive(PType::F32, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
    }

    fn f64_accessor(&self) -> Option<AccessorRef<f64>> {
        match self.dtype() {
            DType::Primitive(PType::F64, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
    }

    fn u8_accessor(&self) -> Option<AccessorRef<u8>> {
        match self.dtype() {
            DType::Primitive(PType::U8, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
    }

    fn u16_accessor(&self) -> Option<AccessorRef<u16>> {
        match self.dtype() {
            DType::Primitive(PType::U16, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
    }

    fn u32_accessor(&self) -> Option<AccessorRef<u32>> {
        match self.dtype() {
            DType::Primitive(PType::U32, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
    }

    fn u64_accessor(&self) -> Option<AccessorRef<u64>> {
        match self.dtype() {
            DType::Primitive(PType::U64, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
    }

    fn i8_accessor(&self) -> Option<AccessorRef<i8>> {
        match self.dtype() {
            DType::Primitive(PType::I8, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
    }

    fn i16_accessor(&self) -> Option<AccessorRef<i16>> {
        match self.dtype() {
            DType::Primitive(PType::I16, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
    }

    fn i32_accessor(&self) -> Option<AccessorRef<i32>> {
        match self.dtype() {
            DType::Primitive(PType::I32, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
    }

    fn i64_accessor(&self) -> Option<AccessorRef<i64>> {
        match self.dtype() {
            DType::Primitive(PType::I64, _) => {
                let accessor = Arc::new(self.clone());
                Some(accessor)
            }
            _ => None,
        }
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

#[cfg(test)]
mod tests {
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
}
