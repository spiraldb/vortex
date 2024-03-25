use crate::array::{Array, ArrayRef};
use crate::compute::scalar_at::scalar_at;
use vortex_error::{VortexError, VortexResult};
use vortex_schema::{DType, Nullability};

#[derive(Debug, Clone)]
pub struct Validity {
    array: Option<ArrayRef>,
}

impl Validity {
    pub fn new(array: Option<ArrayRef>) -> Self {
        if let Some(a) = &array {
            if !matches!(a.dtype(), &DType::Bool(Nullability::NonNullable)) {
                panic!("Validity array must be of type bool");
            }
        }
        Self { array }
    }

    pub fn invalid(array: ArrayRef) -> Self {
        Self::new(Some(array))
    }

    pub fn valid() -> Self {
        Self::new(None)
    }

    pub fn check_length(&self, length: usize) -> VortexResult<()> {
        if let Some(a) = &self.array {
            if a.len() != length {
                return Err(VortexError::InvalidArgument(
                    format!(
                        "Validity buffer {} has incorrect length {}, expected {}",
                        a,
                        a.len(),
                        length
                    )
                    .into(),
                ));
            }
        }
        Ok(())
    }
}

impl From<&Validity> for Nullability {
    fn from(value: &Validity) -> Self {
        value.array.is_some().into()
    }
}

pub trait ArrayValidity {
    fn validity(&self) -> Validity;

    fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .array
            .map(|a| scalar_at(&a, index).unwrap())
            .map(|s| s.try_into().unwrap())
            .unwrap_or(true)
    }
}
