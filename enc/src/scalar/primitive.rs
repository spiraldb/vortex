use crate::scalar::Scalar;
use crate::types::{DType, PType, PrimitiveType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimitiveScalar<T: PrimitiveType> {
    value: T,
    dtype: DType,
}

impl<T: PrimitiveType> PrimitiveScalar<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            value,
            dtype: T::PTYPE.into(),
        }
    }

    #[inline]
    pub fn value(&self) -> T {
        self.value
    }

    #[inline]
    pub fn ptype() -> PType {
        T::PTYPE
    }
}

impl<T: PrimitiveType> Scalar for PrimitiveScalar<T> {
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
        impl From<$T> for PrimitiveScalar<$T> {
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
            type Error = ();

            #[inline]
            fn try_from(value: Box<dyn Scalar>) -> Result<Self, Self::Error> {
                value.as_ref().try_into()
            }
        }

        impl TryFrom<&dyn Scalar> for $T {
            type Error = ();

            fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
                match value.as_any().downcast_ref::<PrimitiveScalar<$T>>() {
                    Some(scalar) => Ok(scalar.value()),
                    None => Err(()),
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
primitive_scalar_from!(f32);
primitive_scalar_from!(f64);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn into_from() {
        let scalar: Box<dyn Scalar> = (10u16).into();
        assert_eq!(scalar.as_ref().try_into(), Ok(10u16));
    }
}
