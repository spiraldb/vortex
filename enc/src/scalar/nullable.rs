use std::any::Any;
use std::fmt::{Display, Formatter};
use std::mem::size_of;

use crate::dtype::DType;
use crate::error::{EncError, EncResult};
use crate::scalar::{NullScalar, Scalar};

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum NullableScalar {
    None(DType),
    Some(Box<dyn Scalar>, DType),
}

impl NullableScalar {
    pub fn some(scalar: Box<dyn Scalar>) -> Self {
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
    fn boxed(self) -> Box<dyn Scalar> {
        Box::new(self)
    }
    #[inline]
    fn dtype(&self) -> &DType {
        match self {
            Self::Some(_, dtype) => dtype,
            Self::None(dtype) => dtype,
        }
    }

    fn cast(&self, _dtype: &DType) -> EncResult<Box<dyn Scalar>> {
        todo!()
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

impl<T: Into<Box<dyn Scalar>>> From<NullableScalarOption<T>> for Box<dyn Scalar> {
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

impl<T: TryFrom<Box<dyn Scalar>, Error = EncError>> TryFrom<&dyn Scalar>
    for NullableScalarOption<T>
{
    type Error = EncError;

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        let Some(ns) = value.as_any().downcast_ref::<NullableScalar>() else {
            return Err(EncError::InvalidDType(value.dtype().clone()));
        };

        Ok(NullableScalarOption(match ns {
            NullableScalar::None(_) => None,
            NullableScalar::Some(v, _) => Some(v.clone().try_into()?),
        }))
    }
}

impl<T: TryFrom<Box<dyn Scalar>, Error = EncError>> TryFrom<Box<dyn Scalar>>
    for NullableScalarOption<T>
{
    type Error = EncError;

    fn try_from(value: Box<dyn Scalar>) -> Result<Self, Self::Error> {
        let dtype = value.dtype().clone();
        let ns = value
            .into_any()
            .downcast::<NullableScalar>()
            .map_err(|_| EncError::InvalidDType(dtype))?;

        Ok(NullableScalarOption(match *ns {
            NullableScalar::None(_) => None,
            NullableScalar::Some(v, _) => Some(v.try_into()?),
        }))
    }
}
