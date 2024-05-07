use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError};

use crate::scalar2::Scalar;

pub struct StructScalar<'a>(&'a Scalar);
impl<'a> StructScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    pub fn field_by_idx(&self, idx: usize, dtype: DType) -> Option<Scalar> {
        self.0.value.child(idx).map(|value| Scalar { dtype, value })
    }

    pub fn field(&self, name: &str, dtype: DType) -> Option<Scalar> {
        let DType::Struct(struct_dtype, _) = self.0.dtype() else {
            unreachable!()
        };
        struct_dtype
            .find_name(name)
            .and_then(|idx| self.field_by_idx(idx, dtype))
    }
}

impl<'a> TryFrom<&'a Scalar> for StructScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if matches!(value.dtype(), DType::Struct(..)) {
            Ok(Self(value))
        } else {
            vortex_bail!("Expected struct scalar, found {}", value.dtype())
        }
    }
}
