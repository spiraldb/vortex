use std::marker::PhantomData;

use vortex_dtype::{DType, NativePType, PType};
use vortex_error::{vortex_bail, VortexError};

use crate::value::ScalarView;
use crate::Scalar;

pub struct PrimitiveScalar<'a, T: NativePType + for<'b> From<&'b ScalarView>>(
    &'a Scalar,
    PhantomData<T>,
);
impl<'a, T: NativePType + for<'b> From<&'b ScalarView>> PrimitiveScalar<'a, T> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        T::PTYPE
    }

    pub fn value(&self) -> Option<T> {
        self.0.value.as_primitive::<T>()
    }
}

impl<'a, T: NativePType + for<'b> From<&'b ScalarView>> TryFrom<&'a Scalar>
    for PrimitiveScalar<'a, T>
{
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if matches!(value.dtype(), DType::Primitive(p, _) if p == &T::PTYPE) {
            Ok(Self(value, Default::default()))
        } else {
            vortex_bail!(
                "Expected scalar of type {}, found {}",
                T::PTYPE,
                value.dtype()
            )
        }
    }
}
