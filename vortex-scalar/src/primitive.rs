use num_traits::NumCast;
use vortex_dtype::half::f16;
use vortex_dtype::{match_each_native_ptype, DType, NativePType, Nullability, PType};
use vortex_error::{
    vortex_bail, vortex_err, vortex_panic, VortexError, VortexResult, VortexUnwrap,
};

use crate::pvalue::PValue;
use crate::value::ScalarValue;
use crate::Scalar;

#[derive(Debug, Clone)]
pub struct PrimitiveScalar<'a> {
    dtype: &'a DType,
    ptype: PType,
    pvalue: Option<PValue>,
}

impl<'a> PrimitiveScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.dtype
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.ptype
    }

    pub fn typed_value<T: NativePType + TryFrom<PValue, Error = VortexError>>(&self) -> Option<T> {
        assert_eq!(
            self.ptype,
            T::PTYPE,
            "Attempting to read {} scalar as {}",
            self.ptype,
            T::PTYPE
        );

        self.pvalue
            .as_ref()
            .map(|pv| T::try_from(*pv).vortex_unwrap())
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        let ptype = PType::try_from(dtype)?;
        match_each_native_ptype!(ptype, |$Q| {
            match_each_native_ptype!(self.ptype(), |$T| {
                Ok(Scalar::primitive::<$Q>(
                    <$Q as NumCast>::from(self.typed_value::<$T>().expect("Invalid value"))
                        .ok_or_else(|| vortex_err!("Can't cast {} scalar to {}", self.ptype, dtype))?,
                    dtype.nullability(),
                ))
            })
        })
    }
}

impl<'a> TryFrom<&'a Scalar> for PrimitiveScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if !matches!(value.dtype(), DType::Primitive(..)) {
            vortex_bail!("Expected primitive scalar, found {}", value.dtype())
        }

        let ptype = PType::try_from(value.dtype())?;

        // Read the serialized value into the correct PValue.
        // The serialized form may come back over the wire as e.g. any integer type.
        let pvalue = match_each_native_ptype!(ptype, |$T| {
            if let Some(pvalue) = value.value.as_pvalue()? {
                Some(PValue::from(<$T>::try_from(pvalue)?))
            } else {
                None
            }
        });

        Ok(Self {
            dtype: value.dtype(),
            ptype,
            pvalue,
        })
    }
}

impl Scalar {
    pub fn primitive<T: NativePType + Into<PValue>>(value: T, nullability: Nullability) -> Self {
        Self {
            dtype: DType::Primitive(T::PTYPE, nullability),
            value: ScalarValue::Primitive(value.into()),
        }
    }

    pub fn reinterpret_cast(&self, ptype: PType) -> Self {
        let primitive = PrimitiveScalar::try_from(self).unwrap_or_else(|e| {
            vortex_panic!(e, "Failed to reinterpret cast {} to {}", self.dtype, ptype)
        });
        if primitive.ptype() == ptype {
            return self.clone();
        }

        assert_eq!(
            primitive.ptype().byte_width(),
            ptype.byte_width(),
            "can't reinterpret cast between integers of two different widths"
        );

        Scalar::new(
            DType::Primitive(ptype, self.dtype.nullability()),
            primitive
                .pvalue
                .map(|p| p.reinterpret_cast(ptype))
                .map(ScalarValue::Primitive)
                .unwrap_or_else(|| ScalarValue::Null),
        )
    }

    pub fn zero<T: NativePType + Into<PValue>>(nullability: Nullability) -> Self {
        Self {
            dtype: DType::Primitive(T::PTYPE, nullability),
            value: ScalarValue::Primitive(T::zero().into()),
        }
    }
}

macro_rules! primitive_scalar {
    ($T:ty) => {
        impl From<$T> for Scalar {
            fn from(value: $T) -> Self {
                Scalar {
                    dtype: DType::Primitive(<$T>::PTYPE, Nullability::NonNullable),
                    value: ScalarValue::Primitive(value.into()),
                }
            }
        }

        impl From<Option<$T>> for Scalar {
            fn from(value: Option<$T>) -> Self {
                Scalar {
                    dtype: DType::Primitive(<$T>::PTYPE, Nullability::Nullable),
                    value: value
                        .map(|v| ScalarValue::Primitive(v.into()))
                        .unwrap_or_else(|| ScalarValue::Null),
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

        impl TryFrom<Scalar> for $T {
            type Error = VortexError;

            fn try_from(value: Scalar) -> Result<Self, Self::Error> {
                <$T>::try_from(&value)
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
primitive_scalar!(f16);
primitive_scalar!(f32);
primitive_scalar!(f64);

impl From<usize> for Scalar {
    fn from(value: usize) -> Self {
        Self::from(value as u64)
    }
}

/// Read a scalar as usize. For usize only, we implicitly cast for better ergonomics.
impl TryFrom<&Scalar> for usize {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> Result<Self, Self::Error> {
        u64::try_from(
            value
                .cast(&DType::Primitive(PType::U64, Nullability::NonNullable))?
                .as_ref(),
        )
        .map(|v| v as Self)
    }
}
