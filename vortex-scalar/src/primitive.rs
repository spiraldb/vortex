use std::marker::PhantomData;

use vortex_buffer::Buffer;
use vortex_dtype::half::f16;
use vortex_dtype::{DType, NativePType, Nullability, PType};
use vortex_error::{vortex_bail, vortex_err, VortexError};

use crate::value::{ScalarData, ScalarValue, ScalarView};
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

impl From<usize> for Scalar {
    fn from(value: usize) -> Self {
        Scalar::from(value as u64)
    }
}

impl TryFrom<&Scalar> for usize {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> Result<Self, Self::Error> {
        u64::try_from(value).map(|value| value as usize)
    }
}

macro_rules! primitive_scalar {
    ($T:ty) => {
        impl From<$T> for Scalar {
            fn from(value: $T) -> Self {
                Scalar {
                    dtype: DType::Primitive(<$T>::PTYPE, Nullability::NonNullable),
                    value: ScalarValue::Data(ScalarData::from(value)),
                }
            }
        }

        impl TryFrom<&Scalar> for $T {
            type Error = VortexError;

            fn try_from(value: &Scalar) -> Result<Self, Self::Error> {
                PrimitiveScalar::<$T>::try_from(value)?
                    .value()
                    .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
            }
        }
    };
}

primitive_scalar!(u8);
primitive_scalar!(u16);
primitive_scalar!(u32);
primitive_scalar!(u64);
primitive_scalar!(i8);
primitive_scalar!(i16);
primitive_scalar!(i32);
primitive_scalar!(i64);
primitive_scalar!(f32);
primitive_scalar!(f64);

impl From<f16> for Scalar {
    fn from(value: f16) -> Self {
        Scalar {
            dtype: DType::Primitive(PType::F16, Nullability::NonNullable),
            value: ScalarValue::Data(ScalarData::Buffer(Buffer::from(
                value.to_le_bytes().to_vec(),
            ))),
        }
    }
}

impl TryFrom<&Scalar> for f16 {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> Result<Self, Self::Error> {
        PrimitiveScalar::<f16>::try_from(value)?
            .value()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}
