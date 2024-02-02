use codecz_sys::{
    codecz_math_isConstant_f32, codecz_math_isConstant_f64, codecz_math_isConstant_i16,
    codecz_math_isConstant_i32, codecz_math_isConstant_i64, codecz_math_isConstant_i8,
    codecz_math_isConstant_u16, codecz_math_isConstant_u32, codecz_math_isConstant_u64,
    codecz_math_isConstant_u8, codecz_math_isSorted_f32, codecz_math_isSorted_f64,
    codecz_math_isSorted_i16, codecz_math_isSorted_i32, codecz_math_isSorted_i64,
    codecz_math_isSorted_i8, codecz_math_isSorted_u16, codecz_math_isSorted_u32,
    codecz_math_isSorted_u64, codecz_math_isSorted_u8, codecz_math_max_f32, codecz_math_max_f64,
    codecz_math_max_i16, codecz_math_max_i32, codecz_math_max_i64, codecz_math_max_i8,
    codecz_math_max_u16, codecz_math_max_u32, codecz_math_max_u64, codecz_math_max_u8,
    codecz_math_min_f32, codecz_math_min_f64, codecz_math_min_i16, codecz_math_min_i32,
    codecz_math_min_i64, codecz_math_min_i8, codecz_math_min_u16, codecz_math_min_u32,
    codecz_math_min_u64, codecz_math_min_u8, codecz_math_runLengthStats_f32,
    codecz_math_runLengthStats_f64, codecz_math_runLengthStats_i16, codecz_math_runLengthStats_i32,
    codecz_math_runLengthStats_i64, codecz_math_runLengthStats_i8, codecz_math_runLengthStats_u16,
    codecz_math_runLengthStats_u32, codecz_math_runLengthStats_u64, codecz_math_runLengthStats_u8,
    RunLengthStats_t,
};
use num_traits::Num;
use paste;

#[allow(dead_code)]
pub struct RunLengthStats {
    run_count: u64,
    run_element_count: u64,
}

impl From<RunLengthStats_t> for RunLengthStats {
    fn from(value: RunLengthStats_t) -> Self {
        Self {
            run_count: value.runCount,
            run_element_count: value.runElementCount,
        }
    }
}

pub fn max<T: PrimitiveNumber>(elems: &[T]) -> T {
    T::max(elems, private::Sealed)
}

pub fn min<T: PrimitiveNumber>(elems: &[T]) -> T {
    T::min(elems, private::Sealed)
}

pub fn is_constant<T: PrimitiveNumber>(elems: &[T]) -> bool {
    T::is_constant(elems, private::Sealed)
}

pub fn is_sorted<T: PrimitiveNumber>(elems: &[T]) -> bool {
    T::is_sorted(elems, private::Sealed)
}

pub fn run_length_stats<T: PrimitiveNumber>(elems: &[T]) -> RunLengthStats {
    T::run_length_stats(elems, private::Sealed)
}

pub trait PrimitiveNumber: Num {
    fn max(elems: &[Self], _: private::Sealed) -> Self;
    fn min(elems: &[Self], _: private::Sealed) -> Self;
    fn is_constant(elems: &[Self], _: private::Sealed) -> bool;
    fn is_sorted(elems: &[Self], _: private::Sealed) -> bool;
    fn run_length_stats(elems: &[Self], _: private::Sealed) -> RunLengthStats;
}

mod private {
    pub struct Sealed;
}

macro_rules! impl_codecz_math_num {
    ($t:ty) => {
        paste::item! {
            impl PrimitiveNumber for $t {
                fn max(elems: &[Self], _token: private::Sealed) -> Self {
                    unsafe { [<codecz_math_max_ $t>](elems.as_ptr(), elems.len()) as $t }
                }
                fn min(elems: &[Self], _token: private::Sealed) -> Self {
                    unsafe { [<codecz_math_min_ $t>](elems.as_ptr(), elems.len()) as $t }
                }
                fn is_constant(elems: &[Self], _token: private::Sealed) -> bool {
                    unsafe { [<codecz_math_isConstant_ $t>](elems.as_ptr(), elems.len()) }
                }
                fn is_sorted(elems: &[Self], _token: private::Sealed) -> bool {
                    unsafe { [<codecz_math_isSorted_ $t>](elems.as_ptr(), elems.len()) }
                }
                fn run_length_stats(elems: &[Self], _token: private::Sealed) -> RunLengthStats {
                    let mut out = RunLengthStats_t{ runCount: 0, runElementCount: 0 };
                    unsafe {
                        [<codecz_math_runLengthStats_ $t>](
                            elems.as_ptr(),
                            elems.len(),
                            &mut out as *mut RunLengthStats_t
                        )
                    };
                    out.into()
                }
            }
        }
    };
}

impl_codecz_math_num!(f32);
impl_codecz_math_num!(f64);
impl_codecz_math_num!(u8);
impl_codecz_math_num!(u16);
impl_codecz_math_num!(u32);
impl_codecz_math_num!(u64);
impl_codecz_math_num!(i8);
impl_codecz_math_num!(i16);
impl_codecz_math_num!(i32);
impl_codecz_math_num!(i64);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_math() {
        let vec = vec![1.0, 2.0, 3.0];
        let max = self::max(&vec);
        assert_eq!(max, 3.0);

        let min = self::min(&vec);
        assert_eq!(min, 1.0);

        let is_constant = self::is_constant(&vec);
        assert!(!is_constant);

        let is_sorted = self::is_sorted(&vec);
        assert!(is_sorted);

        let run_length_stats = self::run_length_stats(&vec);
        assert_eq!(run_length_stats.run_count, 0);
        assert_eq!(run_length_stats.run_element_count, 0);
    }
}
