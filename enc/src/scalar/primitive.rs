use half::f16;

use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::{match_each_pvalue_integer, DType, PType, PValue, PrimitiveType};

#[derive(Debug, Clone, PartialEq)]
pub struct PrimitiveScalar {
    value: PValue,
    dtype: DType,
}

impl PrimitiveScalar {
    #[inline]
    pub fn new<T: PrimitiveType>(value: T) -> Self {
        Self {
            value: value.pvalue(),
            dtype: T::PTYPE.into(),
        }
    }

    #[inline]
    pub fn value(&self) -> &PValue {
        &self.value
    }

    pub fn ptype(&self) -> PType {
        self.value.ptype()
    }
}

impl Scalar for PrimitiveScalar {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    #[inline]
    fn boxed(self) -> Box<dyn Scalar> {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

macro_rules! primitive_scalar_from {
    ($T:ty) => {
        impl From<$T> for PrimitiveScalar {
            #[inline]
            fn from(value: $T) -> Self {
                Self::new(value)
            }
        }

        impl From<$T> for Box<dyn Scalar> {
            #[inline]
            fn from(value: $T) -> Self {
                Box::new(PrimitiveScalar::new(value))
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
                match value.as_any().downcast_ref::<PrimitiveScalar>() {
                    Some(scalar) => {
                        let v: PValue = scalar.value().clone();
                        v.try_into()
                    }
                    None => Err(EncError::InvalidDType(value.dtype().clone())),
                }
            }
        }
    };
}

// TODO(ngates): I'm sure there's a way to write a macro that loops over all ptypes, but that's
//  beyond me at the moment!
primitive_scalar_from!(u8);
primitive_scalar_from!(u16);
primitive_scalar_from!(u32);
primitive_scalar_from!(u64);
primitive_scalar_from!(i8);
primitive_scalar_from!(i16);
primitive_scalar_from!(i32);
primitive_scalar_from!(i64);
primitive_scalar_from!(f16);
primitive_scalar_from!(f32);
primitive_scalar_from!(f64);

impl TryFrom<&dyn Scalar> for usize {
    type Error = EncError;

    fn try_from(value: &dyn Scalar) -> EncResult<Self> {
        match value.as_any().downcast_ref::<PrimitiveScalar>() {
            Some(scalar) => match_each_pvalue_integer!(scalar.value(), |$V| {
                if is_negative(*$V) {
                    return Err(EncError::ComputeError("required positive integer".into()));
                }
                Ok(*$V as usize)
            }),
            None => Err(EncError::InvalidDType(value.dtype().clone())),
        }
    }
}

#[inline]
fn is_negative<T: Default + PartialOrd>(value: T) -> bool {
    value < T::default()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn into_from() {
        let scalar: Box<dyn Scalar> = (10u16).into();
        assert_eq!(scalar.as_ref().try_into(), Ok(10u16));
        // All integers should be convertible to usize
        assert_eq!(scalar.as_ref().try_into(), Ok(10usize));

        let scalar: Box<dyn Scalar> = (-10i16).into();
        assert_eq!(
            scalar.as_ref().try_into(),
            Err::<usize, EncError>(EncError::ComputeError("required positive integer".into()))
        );
    }
}
