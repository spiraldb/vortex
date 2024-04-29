use std::fmt::{Display, Formatter};

use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::scalar::value::ScalarValue;
use crate::scalar::Scalar;

pub type BoolScalar = ScalarValue<bool>;

impl BoolScalar {
    #[inline]
    pub fn dtype(&self) -> &DType {
        match self.nullability() {
            Nullability::NonNullable => &DType::Bool(Nullability::NonNullable),
            Nullability::Nullable => &DType::Bool(Nullability::Nullable),
        }
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        match dtype {
            DType::Bool(_) => Ok(self.clone().into()),
            _ => Err(vortex_err!(MismatchedTypes: "bool", dtype)),
        }
    }

    pub fn nbytes(&self) -> usize {
        1
    }
}

impl From<bool> for Scalar {
    #[inline]
    fn from(value: bool) -> Self {
        BoolScalar::some(value).into()
    }
}

impl TryFrom<&Scalar> for bool {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> VortexResult<Self> {
        let Scalar::Bool(b) = value else {
            vortex_bail!(MismatchedTypes: "bool", value.dtype());
        };
        b.value()
            .cloned()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}

impl TryFrom<Scalar> for bool {
    type Error = VortexError;

    fn try_from(value: Scalar) -> VortexResult<Self> {
        let Scalar::Bool(b) = value else {
            vortex_bail!(MismatchedTypes: "bool", value.dtype());
        };
        b.into_value()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}

impl Display for BoolScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "null"),
            Some(b) => Display::fmt(&b, f),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn into_from() {
        let scalar: Scalar = false.into();
        assert!(!bool::try_from(scalar).unwrap());
    }
}
