use itertools::Itertools;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexError, VortexResult};

use crate::value::ScalarValue;
use crate::Scalar;

pub struct ListScalar<'a>(&'a Scalar);
impl<'a> ListScalar<'a> {
    #[inline]
    pub fn dtype(&self) -> &'a DType {
        self.0.dtype()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.value.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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

    pub fn cast(&self, _dtype: &DType) -> VortexResult<Scalar> {
        todo!()
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
            value: ScalarValue::List(scalars.into_iter().map(|s| s.value).collect_vec().into()),
        }
    }
}
