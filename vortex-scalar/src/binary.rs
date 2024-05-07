use vortex_buffer::Buffer;
use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::value::ScalarValue;
use crate::Scalar;

pub struct BinaryScalar<'a>(&'a Scalar);
impl<'a> BinaryScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    pub fn value(&self) -> Option<Buffer> {
        self.0.value.as_bytes()
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }
}

impl Scalar {
    pub fn binary(buffer: Buffer, nullability: Nullability) -> Self {
        Scalar {
            dtype: DType::Binary(nullability),
            value: ScalarValue::Buffer(buffer),
        }
    }
}

impl<'a> TryFrom<&'a Scalar> for BinaryScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if matches!(value.dtype(), DType::Binary(_)) {
            Ok(Self(value))
        } else {
            vortex_bail!("Expected binary scalar, found {}", value.dtype())
        }
    }
}

impl<'a> TryFrom<&'a Scalar> for Buffer {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> VortexResult<Self> {
        BinaryScalar::try_from(value)?
            .value()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}
