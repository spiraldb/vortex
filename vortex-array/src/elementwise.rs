use vortex_dtype::{DType, NativePType, PType};
use vortex_error::{VortexError, VortexResult};
use vortex_scalar::Scalar;

use crate::{Array, ArrayDType};

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
        binary_fn: F,
    ) -> VortexResult<Array>;
}

pub trait UnaryFn {
    fn unary<I: NativePType + TryFrom<Scalar, Error = VortexError>, O: NativePType, F: Fn(I) -> O>(
        &self,
        unary_fn: F,
    ) -> VortexResult<Array>;
}

// TODO(adamgs): Turn into a macro, or just have some intermediate adapter struct
pub fn flat_array_iter<I: NativePType>(array: &Array) -> Box<dyn Iterator<Item = I>> {
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
