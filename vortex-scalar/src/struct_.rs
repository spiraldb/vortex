use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

use itertools::Itertools;
use vortex_dtype::{DType, FieldNames, Nullability, StructDType};
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

    pub fn names(&self) -> &FieldNames {
        let DType::Struct(st, _) = self.dtype() else {
            unreachable!("Not a scalar dtype");
        };
        st.names()
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        match dtype {
            DType::Struct(st, n) => {
                // TODO(ngates): check nullability.
                assert_eq!(Nullability::NonNullable, *n);

                if st.dtypes().len() != self.values.len() {
                    vortex_bail!(
                        MismatchedTypes: format!("Struct with {} fields", self.values.len()),
                        dtype
                    );
                }

                let new_fields: Vec<Scalar> = self
                    .values
                    .iter()
                    .zip_eq(st.dtypes().iter())
                    .map(|(field, field_dtype)| field.cast(field_dtype))
                    .try_collect()?;

                let new_type = DType::Struct(
                    StructDType::new(
                        st.names().clone(),
                        new_fields.iter().map(|x| x.dtype().clone()).collect(),
                    ),
                    dtype.nullability(),
                );
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
        let DType::Struct(st, _) = self.dtype() else {
            unreachable!()
        };
        for (n, v) in st.names().iter().zip(self.values.iter()) {
            write!(f, "{} = {}", n, v)?;
        }
        Ok(())
    }
}
