use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use itertools::Itertools;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::Scalar;

#[derive(Debug, Clone, PartialEq)]
pub struct StructScalar {
    dtype: DType,
    values: Vec<Scalar>,
}

impl StructScalar {
    #[inline]
    pub fn new(dtype: DType, values: Vec<Scalar>) -> Self {
        Self { dtype, values }
    }

    #[inline]
    pub fn values(&self) -> &[Scalar] {
        self.values.as_ref()
    }

    #[inline]
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn names(&self) -> &[Arc<String>] {
        let DType::Struct { names, .. } = self.dtype() else {
            unreachable!("Not a scalar dtype");
        };
        names.as_slice()
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        match dtype {
            DType::Struct { names, dtypes } => {
                if dtypes.len() != self.values.len() {
                    vortex_bail!(
                        MismatchedTypes: format!("Struct with {} fields", self.values.len()),
                        dtype
                    );
                }

                let new_fields: Vec<Scalar> = self
                    .values
                    .iter()
                    .zip_eq(dtypes.iter())
                    .map(|(field, field_dtype)| field.cast(field_dtype))
                    .try_collect()?;

                let new_type = DType::Struct {
                    names: names.clone(),
                    dtypes: new_fields.iter().map(|x| x.dtype().clone()).collect(),
                };
                Ok(StructScalar::new(new_type, new_fields).into())
            }
            _ => Err(vortex_err!(MismatchedTypes: "struct", dtype)),
        }
    }

    pub fn nbytes(&self) -> usize {
        self.values().iter().map(|s| s.nbytes()).sum()
    }
}

impl PartialOrd for StructScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.dtype != other.dtype {
            None
        } else {
            self.values.partial_cmp(&other.values)
        }
    }
}

impl Display for StructScalar {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let DType::Struct { names, .. } = self.dtype() else {
            unreachable!()
        };
        for (n, v) in names.iter().zip(self.values.iter()) {
            write!(f, "{} = {}", n, v)?;
        }
        Ok(())
    }
}
