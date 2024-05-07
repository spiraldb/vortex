use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError};
use vortex_flatbuffers::ReadFlatBuffer;

use crate::Scalar;

pub struct ExtScalar<'a>(&'a Scalar);
impl<'a> ExtScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    /// Returns the stored value of the extension scalar.
    pub fn value(&self) -> Option<Scalar> {
        // Need to extract the storage DType from the scalar value.
        // This is stored as a tuple of (dtype, value) with dtype as a serialized.
        let dtype =
            DType::read_flatbuffer_bytes(self.0.value.child(0)?.as_bytes()?.as_ref()).ok()?;
        Some(Scalar {
            dtype,
            value: self.0.value.child(1)?,
        })
    }
}

impl<'a> TryFrom<&'a Scalar> for ExtScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if matches!(value.dtype(), DType::Extension(..)) {
            Ok(Self(value))
        } else {
            vortex_bail!("Expected extension scalar, found {}", value.dtype())
        }
    }
}
