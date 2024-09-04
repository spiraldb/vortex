use vortex_dtype::{DType, NativePType, PType};
use vortex_error::{vortex_bail, VortexError, VortexResult};
use vortex_scalar::Scalar;

use crate::array::PrimitiveArray;
use crate::{Array, ArrayDType, IntoArray};

pub enum OtherValue {
    Scalar(Scalar),
    Array(Array),
}

impl OtherValue {
    pub fn dtype(&self) -> &DType {
        match self {
            Self::Scalar(s) => s.dtype(),
            Self::Array(a) => a.dtype(),
        }
    }
}

impl From<Array> for OtherValue {
    fn from(value: Array) -> Self {
        Self::Array(value)
    }
}

impl From<Scalar> for OtherValue {
    fn from(value: Scalar) -> Self {
        Self::Scalar(value)
    }
}

pub trait BinaryFn {
    fn binary<
        I: NativePType + TryFrom<Scalar, Error = VortexError>,
        O: NativePType,
        F: Fn(I, I) -> O,
    >(
        &self,
        other: OtherValue,
        f: F,
    ) -> VortexResult<Array>;
}

pub trait UnaryFn {
    fn unary<I: NativePType + TryFrom<Scalar, Error = VortexError>, O: NativePType, F: Fn(I) -> O>(
        &self,
        f: F,
    ) -> VortexResult<Array>;
}

impl UnaryFn for PrimitiveArray {
    fn unary<
        I: NativePType + TryFrom<Scalar, Error = VortexError>,
        O: NativePType,
        F: Fn(I) -> O,
    >(
        &self,
        f: F,
    ) -> VortexResult<Array> {
        let mut output = Vec::with_capacity(self.len());

        for v in self.maybe_null_slice::<I>() {
            output.push(f(*v));
        }

        Ok(PrimitiveArray::from_vec(output, self.validity()).into_array())
    }
}

impl BinaryFn for PrimitiveArray {
    fn binary<
        I: NativePType + TryFrom<Scalar, Error = VortexError>,
        O: NativePType,
        F: Fn(I, I) -> O,
    >(
        &self,
        other: OtherValue,
        f: F,
    ) -> VortexResult<Array> {
        if !self.dtype().eq_ignore_nullability(other.dtype()) {
            vortex_bail!(MismatchedTypes: self.dtype(), other.dtype());
        }

        if self.dtype().as_ptype() != Some(&I::PTYPE) {
            vortex_bail!(MismatchedTypes: self.dtype(), I::PTYPE);
        }

        let lhs = self.maybe_null_slice::<I>();
        let mut output = Vec::with_capacity(self.len());

        let validity = match other {
            OtherValue::Scalar(ref s) => {
                let s = I::try_from(s.clone())?;
                for v in lhs {
                    output.push(f(*v, s));
                }
                self.validity()
            }
            OtherValue::Array(ref a) => {
                let rhs_iter = flat_array_iter::<I>(a);
                for (l, r) in lhs.iter().copied().zip(rhs_iter) {
                    output.push(f(l, r));
                }

                let rhs = a.with_dyn(|a| a.logical_validity().into_validity());
                self.validity().and(rhs)?
            }
        };

        Ok(PrimitiveArray::from_vec(output, validity).into_array())
    }
}

// TODO(adamgs): Turn into a macro, or just have some intermediate adapter struct
fn flat_array_iter<I: NativePType>(array: &Array) -> Box<dyn Iterator<Item = I>> {
    match array.dtype().as_ptype().unwrap() {
        PType::U8 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u8_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::U16 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u16_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::U32 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u32_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::U64 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u64_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::I8 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().i8_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::I16 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().i16_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::I32 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().i32_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::I64 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().i64_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::F16 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u64_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::F32 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().f32_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
        PType::F64 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().f64_iter())
                .unwrap()
                .flatten()
                .map(|o| I::from(o.unwrap_or_default()).unwrap()),
        ),
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::validity::Validity;

    #[test]
    fn elementwise_example() {
        let input = PrimitiveArray::from_vec(vec![2u32, 2, 2, 2], Validity::AllValid);

        let o = input
            .binary(
                Scalar::from(2u32).into(),
                |l: u32, r: u32| {
                    if l == r {
                        1_u8
                    } else {
                        0_u8
                    }
                },
            )
            .unwrap();

        let output_iter = o
            .with_dyn(|a| a.as_primitive_array_unchecked().u8_iter())
            .unwrap()
            .flatten();

        for v in output_iter {
            assert_eq!(v.unwrap(), 1);
        }
    }
}
