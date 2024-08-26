use std::sync::Arc;

use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError, VortexResult};

use crate::value::ScalarValue;
use crate::Scalar;

pub struct StructScalar<'a> {
    dtype: &'a DType,
    fields: Option<Arc<[ScalarValue]>>,
}

impl<'a> StructScalar<'a> {
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

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
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
        if !matches!(value.dtype(), DType::Struct(..)) {
            vortex_bail!("Expected struct scalar, found {}", value.dtype())
        }
        Ok(Self {
            dtype: value.dtype(),
            fields: value.value.as_list()?.cloned(),
        })
    }
}
