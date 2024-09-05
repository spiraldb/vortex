use vortex_dtype::NativePType;
use vortex_error::VortexResult;

use crate::Array;

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

#[macro_export]
macro_rules! make_iter_from_array {
    ($array:expr, $tp:ty, | $_:tt $enc:ident | $($body:tt)*) => {{
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}

        use vortex_dtype::PType;
        let ptype = PType::try_from($array.dtype()).unwrap();
        match ptype {
            PType::I8 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().i8_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::I16 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().i16_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::I32 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().i32_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::I64 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().i64_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::U8 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().u8_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::U16 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().u16_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::U32 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().u32_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::U64 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().u64_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::F16 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().f16_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::F32 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().f32_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
            PType::F64 => {
                let iter = $array
                .with_dyn(|a| a.as_primitive_array_unchecked().f64_iter())
                .unwrap()
                .map(|b| b.as_::<$tp>());
                __with__! { iter }
            },
        }
    }};
}
