use half::f16;
use std::any::Any;
use std::mem::size_of;

use crate::error::{EncError, EncResult};
use crate::scalar::{LocalTimeScalar, Scalar};
use crate::types::{DType, PType};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
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

    // General conversion function that handles casting primitive scalar to non primitive.
    // If target dtype can be converted to ptype you should use cast_ptype.
    pub fn cast_dtype(&self, dtype: DType) -> EncResult<Box<dyn Scalar>> {
        macro_rules! from_int {
            ($dtype:ident , $ps:ident) => {
                match $dtype {
                    DType::LocalTime(w) => {
                        Ok(Box::new(LocalTimeScalar::new($ps.clone(), w.clone())))
                    }
                    _ => Err(EncError::InvalidDType($dtype.clone())),
                }
            };
        }

        match self {
            p @ PScalar::U32(_)
            | p @ PScalar::U64(_)
            | p @ PScalar::I32(_)
            | p @ PScalar::I64(_) => from_int!(dtype, p),
            _ => Err(EncError::InvalidDType(dtype.clone())),
        }
    }

    pub fn cast_ptype(&self, ptype: PType) -> EncResult<Box<dyn Scalar>> {
        macro_rules! from_unsigned_int {
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

        macro_rules! from_signed_int {
            ($ptype:ident , $v:ident) => {{
                if is_negative(*$v) {
                    return Err(EncError::ComputeError("required positive integer".into()));
                }
                from_unsigned_int!($ptype, $v)
            }};
        }

        macro_rules! from_floating {
            ($ptype:ident , $v:ident) => {
                match $ptype {
                    PType::F16 => Ok((f16::from_f32(*$v as f32)).into()),
                    PType::F32 => Ok((*$v as f32).into()),
                    PType::F64 => Ok((*$v as f64).into()),
                    _ => Err(EncError::InvalidDType(ptype.into())),
                }
            };
        }

        match self {
            PScalar::U8(v) => from_unsigned_int!(ptype, v),
            PScalar::U16(v) => from_unsigned_int!(ptype, v),
            PScalar::U32(v) => from_unsigned_int!(ptype, v),
            PScalar::U64(v) => from_unsigned_int!(ptype, v),
            PScalar::I8(v) => from_signed_int!(ptype, v),
            PScalar::I16(v) => from_signed_int!(ptype, v),
            PScalar::I32(v) => from_signed_int!(ptype, v),
            PScalar::I64(v) => from_signed_int!(ptype, v),
            PScalar::F16(v) => match ptype {
                PType::F16 => Ok((*v).into()),
                PType::F32 => Ok(v.to_f32().into()),
                PType::F64 => Ok(v.to_f64().into()),
                _ => Err(EncError::InvalidDType(ptype.into())),
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

impl Scalar for PScalar {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    #[inline]
    fn boxed(self) -> Box<dyn Scalar> {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.ptype().into()
    }

    fn cast(&self, dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        let ptype: EncResult<PType> = dtype.try_into();
        ptype
            .and_then(|p| self.cast_ptype(p))
            .or_else(|_| self.cast_dtype(dtype.clone()))
    }

    fn nbytes(&self) -> usize {
        size_of::<Self>()
    }
}

macro_rules! pscalar {
    ($T:ty, $ptype:tt) => {
        impl From<$T> for Box<dyn Scalar> {
            #[inline]
            fn from(value: $T) -> Self {
                Box::new(PScalar::$ptype(value))
            }
        }

        impl TryFrom<Box<dyn Scalar>> for $T {
            type Error = EncError;

            #[inline]
            fn try_from(value: Box<dyn Scalar>) -> EncResult<Self> {
                value.as_ref().try_into()
            }
        }

        impl TryFrom<&dyn Scalar> for $T {
            type Error = EncError;

            fn try_from(value: &dyn Scalar) -> EncResult<Self> {
                match value.as_any().downcast_ref::<PScalar>() {
                    Some(pscalar) => match pscalar {
                        PScalar::$ptype(v) => Ok(*v),
                        _ => Err(EncError::InvalidDType(pscalar.ptype().into())),
                    },
                    None => Err(EncError::InvalidDType(value.dtype().clone())),
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

impl From<usize> for Box<dyn Scalar> {
    #[inline]
    fn from(value: usize) -> Self {
        Box::new(PScalar::U64(value as u64))
    }
}

impl TryFrom<Box<dyn Scalar>> for usize {
    type Error = EncError;

    fn try_from(value: Box<dyn Scalar>) -> EncResult<Self> {
        value.as_ref().try_into()
    }
}

impl TryFrom<&dyn Scalar> for usize {
    type Error = EncError;

    fn try_from(value: &dyn Scalar) -> EncResult<Self> {
        macro_rules! match_each_pscalar_integer {
            ($self:expr, | $_:tt $pscalar:ident | $($body:tt)*) => ({
                macro_rules! __with_pscalar__ {( $_ $pscalar:ident ) => ( $($body)* )}
                match $self {
                    PScalar::U8(v) => __with_pscalar__! { v },
                    PScalar::U16(v) => __with_pscalar__! { v },
                    PScalar::U32(v) => __with_pscalar__! { v },
                    PScalar::U64(v) => __with_pscalar__! { v },
                    PScalar::I8(v) => __with_pscalar__! { v },
                    PScalar::I16(v) => __with_pscalar__! { v },
                    PScalar::I32(v) => __with_pscalar__! { v },
                    PScalar::I64(v) => __with_pscalar__! { v },
                    _ => Err(EncError::InvalidDType($self.ptype().into())),
                }
            })
        }

        match value.as_any().downcast_ref::<PScalar>() {
            Some(pscalar) => match_each_pscalar_integer!(pscalar, |$V| {
                if is_negative(*$V) {
                    return Err(EncError::ComputeError("required positive integer".into()));
                }
                Ok(*$V as usize)
            }),
            None => Err(EncError::InvalidDType(value.dtype().clone())),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::types::{IntWidth, Signedness};

    use super::*;

    #[test]
    fn into_from() {
        let scalar: Box<dyn Scalar> = 10u16.into();
        assert_eq!(scalar.as_ref().try_into(), Ok(10u16));
        // All integers should be convertible to usize
        assert_eq!(scalar.as_ref().try_into(), Ok(10usize));

        let scalar: Box<dyn Scalar> = (-10i16).into();
        assert_eq!(
            scalar.as_ref().try_into(),
            Err::<usize, EncError>(EncError::ComputeError("required positive integer".into()))
        );
    }

    #[test]
    fn cast() {
        let scalar: Box<dyn Scalar> = 10u16.into();
        let u32_scalar = scalar
            .cast(&DType::Int(IntWidth::_32, Signedness::Unsigned))
            .unwrap();
        let u32_scalar_ptype: PType = u32_scalar.dtype().try_into().unwrap();
        assert_eq!(u32_scalar_ptype, PType::U32);
    }
}
