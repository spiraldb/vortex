use std::ops::Deref;
use std::sync::Arc;

use vortex_dtype::field::Field;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexExpect, VortexResult};

use crate::value::ScalarValue;
use crate::Scalar;

pub struct StructScalar<'a> {
    dtype: &'a DType,
    fields: Option<&'a Arc<[ScalarValue]>>,
}

impl<'a> StructScalar<'a> {
    pub fn try_new(dtype: &'a DType, value: &'a ScalarValue) -> VortexResult<Self> {
        if !matches!(dtype, DType::Struct(..)) {
            vortex_bail!("Expected struct scalar, found {}", dtype)
        }
        Ok(Self {
            dtype,
            fields: value.as_list()?,
        })
    }

    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.dtype
    }

    pub fn is_null(&self) -> bool {
        self.fields.is_none()
    }

    pub fn field_by_idx(&self, idx: usize) -> Option<Scalar> {
        let DType::Struct(st, _) = self.dtype() else {
            unreachable!()
        };

        self.fields
            .as_ref()
            .and_then(|fields| fields.get(idx))
            .map(|field| Scalar {
                dtype: st.dtypes()[idx].clone(),
                value: field.clone(),
            })
    }

    pub fn field(&self, name: &str) -> Option<Scalar> {
        let DType::Struct(st, _) = self.dtype() else {
            unreachable!()
        };
        st.find_name(name).and_then(|idx| self.field_by_idx(idx))
    }

    pub fn fields(&self) -> Option<&[ScalarValue]> {
        self.fields.map(Arc::deref)
    }

    pub fn cast(&self, dtype: &DType) -> VortexResult<Scalar> {
        let DType::Struct(st, _) = dtype else {
            vortex_bail!("Can only cast struct to another struct")
        };
        let DType::Struct(own_st, _) = self.dtype() else {
            unreachable!()
        };

        if st.dtypes().len() != own_st.dtypes().len() {
            vortex_bail!(
                "Cannot cast between structs with different number of fields: {} and {}",
                own_st.dtypes().len(),
                st.dtypes().len()
            );
        }

        if let Some(fs) = self.fields() {
            let fields = fs
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    Scalar {
                        dtype: own_st.dtypes()[i].clone(),
                        value: f.clone(),
                    }
                    .cast(&st.dtypes()[i])
                    .map(|s| s.value)
                })
                .collect::<VortexResult<Vec<_>>>()?;
            Ok(Scalar {
                dtype: dtype.clone(),
                value: ScalarValue::List(fields.into()),
            })
        } else {
            Ok(Scalar::null(dtype.clone()))
        }
    }

    pub fn project(&self, projection: &[Field]) -> VortexResult<Scalar> {
        let struct_dtype = self
            .dtype
            .as_struct()
            .ok_or_else(|| vortex_err!("Not a struct dtype"))?;
        let projected_dtype = struct_dtype.project(projection)?;
        let new_fields = if let Some(fs) = self.fields() {
            ScalarValue::List(
                projection
                    .iter()
                    .map(|p| match p {
                        Field::Name(n) => struct_dtype
                            .find_name(n)
                            .vortex_expect("DType has been successfully projected already"),
                        Field::Index(i) => *i,
                    })
                    .map(|i| fs[i].clone())
                    .collect(),
            )
        } else {
            ScalarValue::Null
        };
        Ok(Scalar::new(
            DType::Struct(projected_dtype, self.dtype().nullability()),
            new_fields,
        ))
    }
}

impl Scalar {
    pub fn r#struct(dtype: DType, children: Vec<ScalarValue>) -> Self {
        Self {
            dtype,
            value: ScalarValue::List(children.into()),
        }
    }
}

impl<'a> TryFrom<&'a Scalar> for StructScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        Self::try_new(value.dtype(), &value.value)
    }
}
