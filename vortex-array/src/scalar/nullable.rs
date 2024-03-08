use std::any::Any;
use std::fmt::{Display, Formatter};
use std::mem::size_of;

use crate::dtype::DType;
use crate::error::{VortexError, VortexResult};
use crate::scalar::{NullScalar, Scalar, ScalarRef};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum NullableScalar {
    None(DType),
    Some(ScalarRef, DType),
}

impl NullableScalar {
    pub fn some(scalar: ScalarRef) -> Self {
        let dtype = scalar.dtype().as_nullable();
        Self::Some(scalar, dtype)
    }

    pub fn none(dtype: DType) -> Self {
        Self::None(dtype.as_nullable())
    }
}

impl Scalar for NullableScalar {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    #[inline]
    fn as_nonnull(&self) -> Option<&dyn Scalar> {
        match self {
            Self::Some(s, _) => Some(s.as_ref()),
            Self::None(_) => None,
        }
    }

    #[inline]
    fn into_nonnull(self: Box<Self>) -> Option<ScalarRef> {
        match *self {
            Self::Some(s, _) => Some(s),
            Self::None(_) => None,
        }
    }

    #[inline]
    fn boxed(self) -> ScalarRef {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> &DType {
        match self {
            Self::Some(_, dtype) => dtype,
            Self::None(dtype) => dtype,
        }
    }

    fn cast(&self, dtype: &DType) -> VortexResult<ScalarRef> {
        match self {
            Self::Some(s, _dt) => {
                if dtype.is_nullable() {
                    Ok(Self::Some(s.cast(&dtype.as_nonnullable())?, dtype.clone()).boxed())
                } else {
                    s.cast(&dtype.as_nonnullable())
                }
            }
            Self::None(_dt) => {
                if dtype.is_nullable() {
                    Ok(Self::None(dtype.clone()).boxed())
                } else {
                    Err(VortexError::InvalidDType(dtype.clone()))
                }
            }
        }
    }

    fn nbytes(&self) -> usize {
        match self {
            NullableScalar::Some(s, _) => s.nbytes() + size_of::<DType>(),
            NullableScalar::None(_) => size_of::<DType>(),
        }
    }
}

impl Display for NullableScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NullableScalar::Some(p, _) => write!(f, "{}?", p),
            NullableScalar::None(_) => write!(f, "null"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableScalarOption<T>(pub Option<T>);

impl<T: Into<ScalarRef>> From<NullableScalarOption<T>> for ScalarRef {
    fn from(value: NullableScalarOption<T>) -> Self {
        match value.0 {
            // TODO(robert): This should return NullableScalar::None
            // but that's not possible with some type that holds the associated dtype
            // We need to change the bound of T to be able to get datatype from it.
            None => NullScalar::new().boxed(),
            Some(v) => NullableScalar::some(v.into()).boxed(),
        }
    }
}

impl<T: TryFrom<ScalarRef, Error = VortexError>> TryFrom<&dyn Scalar> for NullableScalarOption<T> {
    type Error = VortexError;

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        let Some(ns) = value.as_any().downcast_ref::<NullableScalar>() else {
            return Err(VortexError::InvalidDType(value.dtype().clone()));
        };

        Ok(NullableScalarOption(match ns {
            NullableScalar::None(_) => None,
            NullableScalar::Some(v, _) => Some(v.clone().try_into()?),
        }))
    }
}

impl<T: TryFrom<ScalarRef, Error = VortexError>> TryFrom<ScalarRef> for NullableScalarOption<T> {
    type Error = VortexError;

    fn try_from(value: ScalarRef) -> Result<Self, Self::Error> {
        let dtype = value.dtype().clone();
        let ns = value
            .into_any()
            .downcast::<NullableScalar>()
            .map_err(|_| VortexError::InvalidDType(dtype))?;

        Ok(NullableScalarOption(match *ns {
            NullableScalar::None(_) => None,
            NullableScalar::Some(v, _) => Some(v.try_into()?),
        }))
    }
}

#[cfg(test)]
mod tests {
    use crate::dtype::DType;
    use crate::ptype::PType;
    use crate::scalar::Scalar;

    #[test]
    fn test_nullable_scalar_option() {
        let ns: Box<dyn Scalar> = Some(10i16).into();
        let nsi32 = ns.cast(&DType::from(PType::I32)).unwrap();
        let v: i32 = nsi32.try_into().unwrap();
        assert_eq!(v, 10);
    }
}
