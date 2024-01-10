use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoolScalar {
    value: bool,
}

impl BoolScalar {
    pub fn new(value: bool) -> Self {
        Self { value }
    }

    pub fn value(&self) -> bool {
        self.value
    }
}

impl Scalar for BoolScalar {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn boxed(self) -> Box<dyn Scalar> {
        Box::new(self)
    }

    #[inline]
    fn dtype(&self) -> DType {
        DType::Bool
    }
}

impl From<bool> for BoolScalar {
    #[inline]
    fn from(value: bool) -> Self {
        Self::new(value)
    }
}

impl From<bool> for Box<dyn Scalar> {
    #[inline]
    fn from(value: bool) -> Self {
        Box::new(BoolScalar::new(value))
    }
}

impl TryFrom<Box<dyn Scalar>> for bool {
    type Error = ();

    #[inline]
    fn try_from(value: Box<dyn Scalar>) -> Result<Self, Self::Error> {
        value.as_ref().try_into()
    }
}

impl TryFrom<&dyn Scalar> for bool {
    type Error = ();

    fn try_from(value: &dyn Scalar) -> Result<Self, Self::Error> {
        match value.as_any().downcast_ref::<BoolScalar>() {
            Some(bool_scalar) => Ok(bool_scalar.value()),
            None => Err(()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn into_from() {
        let scalar: Box<dyn Scalar> = false.into();
        assert_eq!(scalar.as_ref().try_into(), Ok(false));
    }
}
