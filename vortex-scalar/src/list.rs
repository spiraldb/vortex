use std::ops::Deref;
use std::sync::Arc;

use half::f16;
use vortex_dtype::Nullability::NonNullable;
use vortex_dtype::{DType, NativePType, PType};
use vortex_error::{vortex_bail, VortexError, VortexResult};

use crate::value::ScalarValue;
use crate::Scalar;

pub struct ListScalar<'a> {
    dtype: &'a DType,
    elements: Option<Arc<[ScalarValue]>>,
}

impl<'a> ListScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.dtype
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.elements.as_ref().map(|e| e.len()).unwrap_or(0)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        match self.elements.as_ref() {
            None => true,
            Some(l) => l.is_empty(),
        }
    }

    pub fn element_dtype(&self) -> DType {
        let DType::List(element_type, _) = self.dtype() else {
            unreachable!();
        };
        (*element_type).deref().clone()
    }

    pub fn element(&self, idx: usize) -> Option<Scalar> {
        self.elements
            .as_ref()
            .and_then(|l| l.get(idx))
            .map(|value| Scalar {
                dtype: self.element_dtype(),
                value: value.clone(),
            })
    }

    pub fn elements(&self) -> impl Iterator<Item = Scalar> + '_ {
        self.elements
            .as_ref()
            .map(AsRef::as_ref)
            .unwrap_or_else(|| &[] as &[ScalarValue])
            .iter()
            .map(|e| Scalar {
                dtype: self.element_dtype(),
                value: e.clone(),
            })
    }

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
    }
}

impl Scalar {
    pub fn list(element_dtype: DType, children: Vec<ScalarValue>) -> Self {
        Self {
            dtype: DType::List(Arc::new(element_dtype), NonNullable),
            value: ScalarValue::List(children.into()),
        }
    }
}

impl<'a> TryFrom<&'a Scalar> for ListScalar<'a> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        if !matches!(value.dtype(), DType::List(..)) {
            vortex_bail!("Expected list scalar, found {}", value.dtype())
        }

        Ok(Self {
            dtype: value.dtype(),
            elements: value.value.as_list()?.cloned(),
        })
    }
}

impl<'a, T: for<'b> TryFrom<&'b Scalar, Error = VortexError>> TryFrom<&'a Scalar> for Vec<T> {
    type Error = VortexError;

    fn try_from(value: &'a Scalar) -> Result<Self, Self::Error> {
        let value = ListScalar::try_from(value)?;
        let mut elems = vec![];
        for e in value.elements() {
            elems.push(T::try_from(&e)?);
        }
        Ok(elems)
    }
}

macro_rules! from_native_ptype_for_scalar {
    ($T:ty) => {
        impl From<Vec<$T>> for Scalar {
            fn from(value: Vec<$T>) -> Self {
                Self {
                    dtype: DType::List(
                        DType::Primitive(<$T>::PTYPE, NonNullable).into(),
                        NonNullable,
                    ),
                    value: ScalarValue::List(value.into_iter().map(ScalarValue::from).collect()),
                }
            }
        }
    };
}

from_native_ptype_for_scalar!(u8);
from_native_ptype_for_scalar!(u16);
from_native_ptype_for_scalar!(u32);
from_native_ptype_for_scalar!(u64);
from_native_ptype_for_scalar!(i8);
from_native_ptype_for_scalar!(i16);
from_native_ptype_for_scalar!(i32);
from_native_ptype_for_scalar!(i64);
from_native_ptype_for_scalar!(f16);
from_native_ptype_for_scalar!(f32);
from_native_ptype_for_scalar!(f64);

impl From<Vec<usize>> for Scalar {
    fn from(value: Vec<usize>) -> Self {
        Self {
            dtype: DType::List(
                DType::Primitive(PType::U64, NonNullable).into(),
                NonNullable,
            ),
            value: ScalarValue::List(value.into_iter().map(ScalarValue::from).collect()),
        }
    }
}
