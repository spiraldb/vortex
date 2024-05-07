use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError};

use crate::Scalar;

pub struct ListScalar<'a>(&'a Scalar);
impl<'a> ListScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    pub fn element(&self, idx: usize) -> Option<Scalar> {
        let DType::List(element_type, _) = self.dtype() else {
            unreachable!();
        };
        self.0.value.child(idx).map(|value| Scalar {
            dtype: element_type.as_ref().clone(),
            value,
        })
    }
}

impl<'a> TryFrom<&'a Scalar> for ListScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if matches!(value.dtype(), DType::List(..)) {
            Ok(Self(value))
        } else {
            vortex_bail!("Expected list scalar, found {}", value.dtype())
        }
    }
}
