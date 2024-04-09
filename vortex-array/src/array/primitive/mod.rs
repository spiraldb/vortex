use core::cmp::min;
use std::iter;
use std::mem::size_of;
use std::panic::RefUnwindSafe;
use std::ptr::NonNull;
use std::sync::{Arc, RwLock};

use allocator_api2::alloc::Allocator;
use arrow_buffer::buffer::{Buffer, ScalarBuffer};
use itertools::Itertools;
use linkme::distributed_slice;
use num_traits::AsPrimitive;
pub use view::*;
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::accessor::ArrayAccessor;
use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::IntoArray;
use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::iterator::ArrayIter;
use crate::ptype::{match_each_native_ptype, NativePType, PType};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{ArrayStatistics, OwnedStats, Statistics, StatsSet};
use crate::validity::{ArrayValidity, OwnedValidity};
use crate::validity::{Validity, ValidityView};
use crate::view::{AsView, ToOwnedView};
use crate::{impl_array, ArrayWalker};

mod compute;
mod serde;
mod stats;
mod view;

#[derive(Debug, Clone)]
pub struct PrimitiveArray {
    buffer: Buffer,
    ptype: PType,
    dtype: DType,
    validity: Option<Validity>,
    stats: Arc<RwLock<StatsSet>>,
}

impl PrimitiveArray {
    pub fn new(ptype: PType, buffer: Buffer, validity: Option<Validity>) -> Self {
        Self::try_new(ptype, buffer, validity).unwrap()
    }

    pub fn try_new(ptype: PType, buffer: Buffer, validity: Option<Validity>) -> VortexResult<Self> {
        if let Some(v) = validity.as_view() {
            if v.len() != buffer.len() / ptype.byte_width() {
                vortex_bail!("Validity length does not match buffer length");
            }
        }
        let dtype = DType::from(ptype).with_nullability(validity.is_some().into());
        Ok(Self {
            buffer,
            ptype,
            dtype,
            validity,
            stats: Arc::new(RwLock::new(StatsSet::default())),
        })
    }

    /// Allocate buffer from allocator-api2 vector. This would be easier when arrow gets https://github.com/apache/arrow-rs/issues/3960
    #[inline]
    pub fn from_vec_in<T: NativePType, A: Allocator + RefUnwindSafe + Send + Sync + 'static>(
        values: allocator_api2::vec::Vec<T, A>,
    ) -> Self {
        Self::from_nullable_in(values, None)
    }

    pub fn from_nullable_in<
        T: NativePType,
        A: Allocator + RefUnwindSafe + Send + Sync + 'static,
    >(
        values: allocator_api2::vec::Vec<T, A>,
        validity: Option<Validity>,
    ) -> Self {
        let ptr = values.as_ptr();
        let buffer = unsafe {
            Buffer::from_custom_allocation(
                NonNull::new(ptr as _).unwrap(),
                values.len() * size_of::<T>(),
                Arc::new(values),
            )
        };
        Self::new(T::PTYPE, buffer, validity)
    }

    pub fn from_nullable<T: NativePType>(values: Vec<T>, validity: Option<Validity>) -> Self {
        let buffer = Buffer::from_vec::<T>(values);
        Self::new(T::PTYPE, buffer, validity)
    }

    pub fn from_value<T: NativePType>(value: T, n: usize) -> Self {
        PrimitiveArray::from(iter::repeat(value).take(n).collect::<Vec<_>>())
    }

    pub fn null<T: NativePType>(n: usize) -> Self {
        PrimitiveArray::from_nullable(
            iter::repeat(T::zero()).take(n).collect::<Vec<_>>(),
            Some(Validity::Invalid(n)),
        )
    }

    pub fn into_nullable(self, nullability: Nullability) -> Self {
        let dtype = self.dtype().with_nullability(nullability);
        if self.validity().is_some() && nullability == Nullability::NonNullable {
            panic!("Cannot convert nullable array to non-nullable array")
        }
        let len = self.len();
        let validity = if nullability == Nullability::Nullable {
            Some(
                self.validity()
                    .to_owned_view()
                    .unwrap_or_else(|| Validity::Valid(len)),
            )
        } else {
            None
        };
        Self {
            buffer: self.buffer,
            ptype: self.ptype,
            dtype,
            validity,
            stats: self.stats,
        }
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.ptype
    }

    #[inline]
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    pub fn scalar_buffer<T: NativePType>(&self) -> ScalarBuffer<T> {
        ScalarBuffer::from(self.buffer().clone())
    }

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        if self.ptype() != T::PTYPE {
            panic!(
                "Invalid PType! Expected {}, got self.ptype {}",
                T::PTYPE,
                self.ptype()
            );
        }
        self.buffer().typed_data()
    }

    pub fn patch<P: AsPrimitive<usize>, T: NativePType>(
        mut self,
        positions: &[P],
        values: &[T],
    ) -> VortexResult<Self> {
        if self.ptype() != T::PTYPE {
            vortex_bail!(MismatchedTypes: self.dtype, T::PTYPE)
        }

        let mut own_values = self
            .buffer
            .into_vec::<T>()
            .unwrap_or_else(|b| Vec::from(b.typed_data::<T>()));
        // TODO(robert): Also patch validity
        for (idx, value) in positions.iter().zip_eq(values.iter()) {
            own_values[(*idx).as_()] = *value;
        }
        self.buffer = Buffer::from_vec::<T>(own_values);
        Ok(self)
    }

    pub(crate) fn as_trait<T: NativePType>(&self) -> &dyn PrimitiveTrait<T> {
        assert_eq!(self.ptype, T::PTYPE);
        self
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

        PrimitiveArray::new(
            ptype,
            self.buffer().clone(),
            self.validity().to_owned_view(),
        )
    }
}

impl Array for PrimitiveArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.buffer.len() / self.ptype.byte_width()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &PrimitiveEncoding
    }

    fn nbytes(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        match_each_native_ptype!(self.ptype(), |$P| {
            f(&self.as_trait::<$P>())
        })
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        if let Some(v) = self.validity() {
            // FIXME(ngates): should validity implement Array?
            walker.visit_child(&v.to_array())?;
        }
        walker.visit_buffer(self.buffer())
    }
}

impl OwnedValidity for PrimitiveArray {
    fn validity(&self) -> Option<ValidityView> {
        self.validity.as_view()
    }
}

impl OwnedStats for PrimitiveArray {
    fn stats_set(&self) -> &RwLock<StatsSet> {
        &self.stats
    }
}

impl ArrayStatistics for PrimitiveArray {
    fn statistics(&self) -> &dyn Statistics {
        self
    }
}

impl<T: NativePType> PrimitiveTrait<T> for PrimitiveArray {
    fn ptype(&self) -> PType {
        self.ptype
    }

    fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    fn to_primitive(&self) -> PrimitiveArray {
        self.clone()
    }
}

impl<T: NativePType> ArrayAccessor<'_, T> for PrimitiveArray {
    fn value(&self, index: usize) -> Option<T> {
        if self.is_valid(index) {
            Some(self.typed_data::<T>()[index])
        } else {
            None
        }
    }
}

impl PrimitiveArray {
    pub fn iter<T: NativePType>(&self) -> ArrayIter<PrimitiveArray, T> {
        ArrayIter::new(self)
    }
}

#[derive(Debug)]
pub struct PrimitiveEncoding;

impl PrimitiveEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.primitive");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_PRIMITIVE: EncodingRef = &PrimitiveEncoding;

impl Encoding for PrimitiveEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

impl<T: NativePType> From<Vec<T>> for PrimitiveArray {
    fn from(values: Vec<T>) -> Self {
        Self::from_nullable(values, None)
    }
}

impl<T: NativePType> IntoArray for Vec<T> {
    fn into_array(self) -> ArrayRef {
        PrimitiveArray::from(self).into_array()
    }
}

impl<T: NativePType> FromIterator<Option<T>> for PrimitiveArray {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();

        let mut validity: Vec<bool> = Vec::with_capacity(lower);
        let values: Vec<T> = iter
            .map(|i| {
                validity.push(i.is_some());
                i.unwrap_or_default()
            })
            .collect::<Vec<_>>();

        PrimitiveArray::from_nullable(
            values,
            if !validity.is_empty() {
                Some(validity.into())
            } else {
                None
            },
        )
    }
}

impl ArrayDisplay for PrimitiveArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        match_each_native_ptype!(self.ptype(), |$P| {
            f.property("values", format!("{:?}{}",
                &self.buffer().typed_data::<$P>()[..min(10, Array::len(self))],
                if Array::len(self) > 10 { "..." } else { "" }))
        })?;
        f.validity(self.validity())
    }
}

#[cfg(test)]
mod test {
    use vortex_schema::{DType, IntWidth, Nullability, Signedness};

    use crate::array::primitive::PrimitiveArray;
    use crate::array::Array;
    use crate::compute::scalar_at::scalar_at;
    use crate::compute::slice::slice;
    use crate::ptype::PType;

    #[test]
    fn from_arrow() {
        let arr = PrimitiveArray::from(vec![1, 2, 3]);
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.ptype, PType::I32);
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );

        // Ensure we can fetch the scalar at the given index.
        assert_eq!(scalar_at(&arr, 0).unwrap(), 1.into());
        assert_eq!(scalar_at(&arr, 1).unwrap(), 2.into());
        assert_eq!(scalar_at(&arr, 2).unwrap(), 3.into());
    }

    #[test]
    fn slice_array() {
        let arr = slice(&PrimitiveArray::from(vec![1, 2, 3, 4, 5]), 1, 4).unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(scalar_at(&arr, 0).unwrap(), 2.into());
        assert_eq!(scalar_at(&arr, 1).unwrap(), 3.into());
        assert_eq!(scalar_at(&arr, 2).unwrap(), 4.into());
    }
}
