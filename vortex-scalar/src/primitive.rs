use num_traits::NumCast;
use vortex_buffer::Buffer;
use vortex_dtype::half::f16;
use vortex_dtype::{match_each_native_ptype, DType, NativePType, Nullability, PType};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::value::{ScalarData, ScalarValue, ScalarView};
use crate::Scalar;

pub struct PrimitiveScalar<'a>(&'a Scalar);

impl<'a> PrimitiveScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        PType::try_from(self.dtype()).expect("Invalid primitive scalar dtype")
    }

    pub fn typed_value<T: NativePType + for<'b> From<&'b ScalarView>>(&self) -> Option<T> {
        self.0.value.as_primitive::<T>()
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        let ptype = PType::try_from(dtype)?;
        match_each_native_ptype!(ptype, |$Q| {
            match_each_native_ptype!(self.ptype(), |$T| {
                Ok(Scalar::primitive::<$Q>(
                    <$Q as NumCast>::from(self.typed_value::<$T>().expect("Invalid value"))
                        .ok_or_else(|| vortex_err!("Can't cast scalar to {}", dtype))?,
                    dtype.nullability(),
                ))
            })
        })
    }
}

impl<'a> TryFrom<&'a Scalar> for PrimitiveScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if matches!(value.dtype(), DType::Primitive(..)) {
            Ok(Self(value))
        } else {
            vortex_bail!("Expected primitive scalar, found {}", value.dtype())
        }
    }
}

impl Scalar {
    pub fn primitive<T: NativePType>(value: T, nullability: Nullability) -> Scalar {
        Scalar {
            dtype: DType::Primitive(T::PTYPE, nullability),
            value: ScalarValue::Data(ScalarData::Buffer(value.to_le_bytes().into())),
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

        impl From<Option<$T>> for Scalar {
            fn from(value: Option<$T>) -> Self {
                Scalar {
                    dtype: DType::Primitive(<$T>::PTYPE, Nullability::Nullable),
                    value: value
                        .map(|v| ScalarValue::Data(ScalarData::from(v)))
                        .unwrap_or_else(|| ScalarValue::Data(ScalarData::None)),
                }
            }
        }

        impl TryFrom<&Scalar> for $T {
            type Error = VortexError;

            fn try_from(value: &Scalar) -> Result<Self, Self::Error> {
                PrimitiveScalar::try_from(value)?
                    .typed_value::<$T>()
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
        PrimitiveScalar::try_from(value)?
            .typed_value::<f16>()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}
