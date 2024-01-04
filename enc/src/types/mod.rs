pub use dtype::*;
pub use float16::*;
pub use ptype::*;

mod dtype;
mod float16;
mod ptype;

mod private {
    use super::f16;

    pub trait Sealed {}

    impl Sealed for u8 {}

    impl Sealed for u16 {}

    impl Sealed for u32 {}

    impl Sealed for u64 {}

    impl Sealed for i8 {}

    impl Sealed for i16 {}

    impl Sealed for i32 {}

    impl Sealed for i64 {}

    impl Sealed for i128 {}

    impl Sealed for f16 {}

    impl Sealed for f32 {}

    impl Sealed for f64 {}
}
