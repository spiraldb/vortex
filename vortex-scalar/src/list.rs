use itertools::Itertools;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError};

use crate::value::{ScalarData, ScalarValue};
use crate::Scalar;

pub struct ListScalar<'a>(&'a Scalar);
impl<'a> ListScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    pub fn len(&self) -> usize {
        self.0.value.len()
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

    pub fn elements(&self) -> impl Iterator<Item = Scalar> + '_ {
        (0..self.len()).map(move |idx| self.element(idx).expect("incorrect length"))
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

impl<T> From<Vec<T>> for Scalar
where
    Scalar: From<T>,
{
    fn from(value: Vec<T>) -> Self {
        let scalars = value.into_iter().map(|v| Scalar::from(v)).collect_vec();
        let dtype = scalars.first().expect("Empty list").dtype().clone();
        Scalar {
            dtype,
            value: ScalarValue::Data(ScalarData::List(
                scalars
                    .into_iter()
                    .map(|s| s.into_data().expect("shouldn't be a scalar view"))
                    .collect_vec()
                    .into(),
            )),
        }
    }
}
