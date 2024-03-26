use std::fmt::{Display, Formatter};

use vortex_error::{VortexError, VortexResult};
use vortex_schema::{DType, Nullability::NonNullable, Nullability::Nullable};

use crate::scalar::value::ScalarValue;
use crate::scalar::Scalar;

pub type Utf8Scalar = ScalarValue<String>;

impl Utf8Scalar {
    #[inline]
    pub fn dtype(&self) -> &DType {
        match self.nullability() {
            NonNullable => &DType::Utf8(NonNullable),
            Nullable => &DType::Utf8(Nullable),
        }
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        self.value().map(|v| v.len()).unwrap_or(0)
    }
}

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Utf8Scalar::some(value).into()
    }
}

impl From<&str> for Scalar {
    fn from(value: &str) -> Self {
        Utf8Scalar::some(value.to_string()).into()
    }
}

impl TryFrom<Scalar> for String {
    type Error = VortexError;

    fn try_from(value: Scalar) -> Result<Self, Self::Error> {
        let Scalar::Utf8(u) = value else {
            return Err(VortexError::InvalidDType(value.dtype().clone()));
        };
        match u.into_value() {
            None => Err(VortexError::InvalidDType(u.dtype().clone())),
            Some(s) => Ok(s),
        }
    }
}

impl TryFrom<&Scalar> for String {
    type Error = VortexError;

    fn try_from(value: &Scalar) -> Result<Self, Self::Error> {
        let Scalar::Utf8(u) = value else {
            return Err(VortexError::InvalidDType(value.dtype().clone()));
        };
        match u.value() {
            None => Err(VortexError::InvalidDType(u.dtype().clone())),
            Some(s) => Ok(s.to_string()),
        }
    }
}

impl Display for Utf8Scalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "<none>"),
            Some(v) => Display::fmt(v, f),
        }
    }
}
