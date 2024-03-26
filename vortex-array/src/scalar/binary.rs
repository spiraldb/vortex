use std::fmt::{Display, Formatter};

use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;
use vortex_schema::Nullability::{NonNullable, Nullable};

use crate::scalar::value::ScalarValue;
use crate::scalar::Scalar;

pub type BinaryScalar = ScalarValue<Vec<u8>>;

impl BinaryScalar {
    #[inline]
    pub fn dtype(&self) -> &DType {
        match self.nullability() {
            NonNullable => &DType::Binary(NonNullable),
            Nullable => &DType::Binary(Nullable),
        }
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }

    pub fn nbytes(&self) -> usize {
        self.value().map(|s| s.len()).unwrap_or(1)
    }
}

impl From<Vec<u8>> for Scalar {
    fn from(value: Vec<u8>) -> Self {
        BinaryScalar::some(value).into()
    }
}

impl TryFrom<Scalar> for Vec<u8> {
    type Error = VortexError;

    fn try_from(value: Scalar) -> VortexResult<Self> {
        let Scalar::Binary(b) = value else {
            return Err(VortexError::InvalidDType(value.dtype().clone()));
        };
        let dtype = b.dtype().clone();
        b.value()
            .cloned()
            .ok_or_else(|| VortexError::InvalidDType(dtype))
    }
}

impl Display for BinaryScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            None => write!(f, "bytes[none]"),
            Some(b) => write!(f, "bytes[{}]", b.len()),
        }
    }
}
