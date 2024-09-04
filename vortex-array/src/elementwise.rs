use vortex_dtype::{DType, NativePType, PType};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::iter::Batch;
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
    fn binary<I: NativePType, U: NativePType, O: NativePType, F: Fn(I, U) -> O>(
        &self,
        rhs: Array,
        binary_fn: F,
    ) -> VortexResult<Array>;
}

pub trait UnaryFn {
    fn unary<I: NativePType, O: NativePType, F: Fn(I) -> O>(
        &self,
        unary_fn: F,
    ) -> VortexResult<Array>;
}

// TODO(adamgs): Turn into a macro, or just have some intermediate adapter struct
pub fn flat_array_iter<N: NativePType>(array: &Array) -> Box<dyn Iterator<Item = Batch<N>>> {
    match PType::try_from(array.dtype()).unwrap() {
        PType::U8 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u8_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::U16 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u16_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::U32 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u32_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::U64 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u64_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::I8 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().i8_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::I16 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().i16_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::I32 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().i32_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::I64 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().i64_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::F16 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().u64_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::F32 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().f32_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
        PType::F64 => Box::new(
            array
                .with_dyn(|a| a.as_primitive_array_unchecked().f64_iter())
                .unwrap()
                .map(|b| b.as_::<N>()),
        ),
    }
}
