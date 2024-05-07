use vortex_buffer::BufferString;
use vortex_dtype::DType;
use vortex_dtype::Nullability;
use vortex_dtype::Nullability::NonNullable;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::value::{ScalarData, ScalarValue};
use crate::Scalar;

pub struct Utf8Scalar<'a>(&'a Scalar);
impl<'a> Utf8Scalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    pub fn value(&self) -> Option<BufferString> {
        self.0
            .value
            .as_bytes()
            // Checked on construction that the buffer is valid UTF-8
            .map(|buffer| unsafe { BufferString::new_unchecked(buffer) })
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }
}

impl Scalar {
    pub fn utf8<B>(str: B, nullability: Nullability) -> Self
    where
        BufferString: From<B>,
    {
        Scalar {
            dtype: DType::Utf8(nullability),
            value: ScalarValue::Data(ScalarData::Buffer(BufferString::from(str).into())),
        }
    }
}

impl<'a> TryFrom<&'a Scalar> for Utf8Scalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if matches!(value.dtype(), DType::Utf8(_)) {
            // Validate here that the buffer is indeed UTF-8
            if let Some(buffer) = value.value.as_bytes() {
                let _ = std::str::from_utf8(buffer.as_ref())?;
            }
            Ok(Self(value))
        } else {
            vortex_bail!("Expected utf8 scalar, found {}", value.dtype())
        }
    }
}

impl<'a> TryFrom<&'a Scalar> for BufferString {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> VortexResult<Self> {
        Utf8Scalar::try_from(value)?
            .value()
            .ok_or_else(|| vortex_err!("Can't extract present value from null scalar"))
    }
}

impl From<&str> for Scalar {
    fn from(value: &str) -> Self {
        Scalar {
            dtype: DType::Utf8(NonNullable),
            value: ScalarValue::Data(ScalarData::Buffer(value.as_bytes().into())),
        }
    }
}
