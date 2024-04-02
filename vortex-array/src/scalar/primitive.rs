use std::any;
use std::fmt::{Display, Formatter};
use std::mem::size_of;

use half::f16;

use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::ptype::{NativePType, PType};
use crate::scalar::Scalar;
use crate::{match_each_integer_ptype, match_each_native_ptype};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct PrimitiveScalar {
    ptype: PType,
    dtype: DType,
    nullability: Nullability,
    value: Option<PScalar>,
}

impl PrimitiveScalar {
    pub fn try_new<T: NativePType>(
        value: Option<T>,
        nullability: Nullability,
    ) -> VortexResult<Self> {
        if value.is_none() && nullability == Nullability::NonNullable {
            vortex_bail!("Value cannot be None for NonNullable Scalar");
        }
        Ok(Self {
            ptype: T::PTYPE,
            dtype: DType::from(T::PTYPE).with_nullability(nullability),
            nullability,
            value: value.map(|v| Into::<PScalar>::into(v)),
        })
    }

    pub fn nullable<T: NativePType>(value: Option<T>) -> Self {
        Self::try_new(value, Nullability::Nullable).unwrap()
    }

    pub fn some<T: NativePType>(value: T) -> Self {
        Self::try_new::<T>(Some(value), Nullability::default()).unwrap()
    }

    pub fn none<T: NativePType>() -> Self {
        Self::try_new::<T>(None, Nullability::Nullable).unwrap()
    }

    #[inline]
    pub fn value(&self) -> Option<PScalar> {
        self.value
    }

    pub fn typed_value<T: NativePType>(&self) -> Option<T> {
        assert_eq!(
            T::PTYPE,
            self.ptype,
            "typed_value called with incorrect ptype"
        );
        self.value.map(|v| v.try_into().unwrap())
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.ptype
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        let ptype: PType = dtype.try_into()?;
        match_each_native_ptype!(ptype, |$T| {
            Ok(PrimitiveScalar::try_new(
                self.value()
                .map(|ps| ps.cast_ptype(ptype))
                .transpose()?
                .map(|s| $T::try_from(s))
                .transpose()?,
                self.nullability,
            )?.into())
        })
    }

    pub fn nbytes(&self) -> usize {
        size_of::<Self>()
    }
}

impl Display for PrimitiveScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "<none>({}?)", self.ptype),
            Some(v) => write!(f, "{}({})", v, self.ptype),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub enum PScalar {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F16(f16),
    F32(f32),
    F64(f64),
}

impl PScalar {
    pub fn ptype(&self) -> PType {
        match self {
            PScalar::U8(_) => PType::U8,
            PScalar::U16(_) => PType::U16,
            PScalar::U32(_) => PType::U32,
            PScalar::U64(_) => PType::U64,
            PScalar::I8(_) => PType::I8,
            PScalar::I16(_) => PType::I16,
            PScalar::I32(_) => PType::I32,
            PScalar::I64(_) => PType::I64,
            PScalar::F16(_) => PType::F16,
            PScalar::F32(_) => PType::F32,
            PScalar::F64(_) => PType::F64,
        }
    }

    pub fn cast_ptype(&self, ptype: PType) -> VortexResult<Scalar> {
        macro_rules! from_int {
            ($ptype:ident , $v:ident) => {
                match $ptype {
                    PType::U8 => Ok((*$v as u8).into()),
                    PType::U16 => Ok((*$v as u16).into()),
                    PType::U32 => Ok((*$v as u32).into()),
                    PType::U64 => Ok((*$v as u64).into()),
                    PType::I8 => Ok((*$v as i8).into()),
                    PType::I16 => Ok((*$v as i16).into()),
                    PType::I32 => Ok((*$v as i32).into()),
                    PType::I64 => Ok((*$v as i64).into()),
                    PType::F16 => Ok(f16::from_f32(*$v as f32).into()),
                    PType::F32 => Ok((*$v as f32).into()),
                    PType::F64 => Ok((*$v as f64).into()),
                }
            };
        }

        macro_rules! from_floating {
            ($ptype:ident , $v:ident) => {
                match $ptype {
                    PType::F16 => Ok((f16::from_f32(*$v as f32)).into()),
                    PType::F32 => Ok((*$v as f32).into()),
                    PType::F64 => Ok((*$v as f64).into()),
                    _ => Err(vortex_err!(mt = "any float", ptype)),
                }
            };
        }

        match self {
            PScalar::U8(v) => from_int!(ptype, v),
            PScalar::U16(v) => from_int!(ptype, v),
            PScalar::U32(v) => from_int!(ptype, v),
            PScalar::U64(v) => from_int!(ptype, v),
            PScalar::I8(v) => from_int!(ptype, v),
            PScalar::I16(v) => from_int!(ptype, v),
            PScalar::I32(v) => from_int!(ptype, v),
            PScalar::I64(v) => from_int!(ptype, v),
            PScalar::F16(v) => match ptype {
                PType::F16 => Ok((*v).into()),
                PType::F32 => Ok(v.to_f32().into()),
                PType::F64 => Ok(v.to_f64().into()),
                _ => Err(vortex_err!(mt = "any float", ptype)),
            },
            PScalar::F32(v) => from_floating!(ptype, v),
            PScalar::F64(v) => from_floating!(ptype, v),
        }
    }
}

#[inline]
fn is_negative<T: Default + PartialOrd>(value: T) -> bool {
    value < T::default()
}

macro_rules! pscalar {
    ($T:ty, $ptype:tt) => {
        impl From<$T> for PScalar {
            fn from(value: $T) -> Self {
                PScalar::$ptype(value)
            }
        }

        impl From<$T> for Scalar {
            fn from(value: $T) -> Self {
                PrimitiveScalar::some(value).into()
            }
        }

        impl TryFrom<&Scalar> for $T {
            type Error = VortexError;

            fn try_from(value: &Scalar) -> VortexResult<Self> {
                match value {
                    Scalar::Primitive(PrimitiveScalar {
                        value: Some(pscalar),
                        ..
                    }) => match pscalar {
                        PScalar::$ptype(v) => Ok(*v),
                        _ => Err(vortex_err!(mt = "$T", pscalar.ptype())),
                    },
                    _ => Err(vortex_err!("can't extract $T from scalar: {}", value)),
                }
            }
        }

        impl TryFrom<Scalar> for $T {
            type Error = VortexError;

            fn try_from(value: Scalar) -> VortexResult<Self> {
                match value {
                    Scalar::Primitive(PrimitiveScalar {
                        value: Some(pscalar),
                        ..
                    }) => pscalar.try_into(),
                    _ => Err(vortex_err!(
                        "Can't extract value of type {} from primitive scalar: {}",
                        any::type_name::<Self>(),
                        value
                    )),
                }
            }
        }

        impl TryFrom<PScalar> for $T {
            type Error = VortexError;

            fn try_from(value: PScalar) -> Result<Self, Self::Error> {
                match value {
                    PScalar::$ptype(v) => Ok(v),
                    _ => Err(vortex_err!(
                        "Expected {} type but got {}",
                        any::type_name::<Self>(),
                        value
                    )),
                }
            }
        }
    };
}

pscalar!(u8, U8);
pscalar!(u16, U16);
pscalar!(u32, U32);
pscalar!(u64, U64);
pscalar!(i8, I8);
pscalar!(i16, I16);
pscalar!(i32, I32);
pscalar!(i64, I64);
pscalar!(f16, F16);
pscalar!(f32, F32);
pscalar!(f64, F64);

impl<T: NativePType> From<Option<T>> for Scalar {
    fn from(value: Option<T>) -> Self {
        PrimitiveScalar::nullable(value).into()
    }
}

impl From<usize> for Scalar {
    #[inline]
    fn from(value: usize) -> Self {
        PrimitiveScalar::some::<u64>(value as u64).into()
    }
}

impl TryFrom<Scalar> for usize {
    type Error = VortexError;

    fn try_from(value: Scalar) -> VortexResult<Self> {
        match value {
            Scalar::Primitive(p) => match_each_integer_ptype!(p.ptype(), |$V| {
                match p.typed_value::<$V>() {
                    None => Err(vortex_err!(ComputeError: "required non null scalar")),
                    Some(v) => {
                        if is_negative(v) {
                            vortex_bail!(ComputeError: "required positive integer");
                        }
                        Ok(v as usize)
                    }
                }
            }),
            _ => Err(vortex_err!("can't extract usize out of scalar: {}", value)),
        }
    }
}

impl TryFrom<&Scalar> for usize {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> VortexResult<Self> {
        match value {
            Scalar::Primitive(p) => match_each_integer_ptype!(p.ptype(), |$V| {
                match p.typed_value::<$V>() {
                    None => Err(vortex_err!(ComputeError: "required non null scalar")),
                    Some(v) => {
                        if is_negative(v) {
                            vortex_bail!(ComputeError: "required positive integer");
                        }
                        Ok(v as usize)
                    }
                }
            }),
            _ => Err(vortex_err!("can't extract usize out of scalar: {}", value)),
        }
    }
}

impl Display for PScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PScalar::U8(p) => Display::fmt(p, f),
            PScalar::U16(p) => Display::fmt(p, f),
            PScalar::U32(p) => Display::fmt(p, f),
            PScalar::U64(p) => Display::fmt(p, f),
            PScalar::I8(p) => Display::fmt(p, f),
            PScalar::I16(p) => Display::fmt(p, f),
            PScalar::I32(p) => Display::fmt(p, f),
            PScalar::I64(p) => Display::fmt(p, f),
            PScalar::F16(p) => Display::fmt(p, f),
            PScalar::F32(p) => Display::fmt(p, f),
            PScalar::F64(p) => Display::fmt(p, f),
        }
    }
}

#[cfg(test)]
mod test {
    use vortex_error::VortexError;
    use vortex_schema::{DType, IntWidth, Nullability, Signedness};

    use crate::ptype::PType;
    use crate::scalar::Scalar;

    #[test]
    fn into_from() {
        let scalar: Scalar = 10u16.into();
        assert_eq!(u16::try_from(scalar.clone()).unwrap(), 10u16);
        // All integers should be convertible to usize
        assert_eq!(usize::try_from(scalar).unwrap(), 10usize);

        let scalar: Scalar = (-10i16).into();
        let error = usize::try_from(scalar).err().unwrap();
        let VortexError::ComputeError(s, _) = error else {
            unreachable!()
        };
        assert_eq!(s.to_string(), "required positive integer");
    }

    #[test]
    fn cast() {
        let scalar: Scalar = 10u16.into();
        let u32_scalar = scalar
            .cast(&DType::Int(
                IntWidth::_32,
                Signedness::Unsigned,
                Nullability::NonNullable,
            ))
            .unwrap();
        let u32_scalar_ptype: PType = u32_scalar.dtype().try_into().unwrap();
        assert_eq!(u32_scalar_ptype, PType::U32);
    }
}
