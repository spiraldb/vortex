use num_traits::Num;
use paste;
use zenc_sys::*;

pub struct RunLengthStats {
    pub run_count: u64,
    pub run_element_count: u64,
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

macro_rules! impl_zimd_num {
    ($t:ty) => {
        paste::item! {
            impl PrimitiveNumber for $t {
                fn max(elems: &[Self], _token: private::Sealed) -> Self {
                    unsafe { [<zimd_max_ $t>](elems.as_ptr(), elems.len()) as $t }
                }
                fn min(elems: &[Self], _token: private::Sealed) -> Self {
                    unsafe { [<zimd_min_ $t>](elems.as_ptr(), elems.len()) as $t }
                }
                fn is_constant(elems: &[Self], _token: private::Sealed) -> bool {
                    unsafe { [<zimd_isConstant_ $t>](elems.as_ptr(), elems.len()) }
                }
                fn is_sorted(elems: &[Self], _token: private::Sealed) -> bool {
                    unsafe { [<zimd_isSorted_ $t>](elems.as_ptr(), elems.len()) }
                }
                fn run_length_stats(elems: &[Self], _token: private::Sealed) -> RunLengthStats {
                    let stats = unsafe {
                        [<zimd_runLengthStats_ $t>](
                            elems.as_ptr(),
                            elems.len(),
                        )
                    };
                    RunLengthStats{ run_count: stats.runCount, run_element_count: stats.runElementCount }
                }
            }
        }
    };
}

impl_zimd_num!(f32);
impl_zimd_num!(f64);
impl_zimd_num!(u8);
impl_zimd_num!(u16);
impl_zimd_num!(u32);
impl_zimd_num!(u64);
impl_zimd_num!(i8);
impl_zimd_num!(i16);
impl_zimd_num!(i32);
impl_zimd_num!(i64);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_math() {
        let vec = vec![1.0, 2.0, 3.0];
        let max = max(&vec);
        assert_eq!(max, 3.0);

        let min = min(&vec);
        assert_eq!(min, 1.0);

        let is_constant = is_constant(&vec);
        assert!(!is_constant);

        let is_sorted = is_sorted(&vec);
        assert!(is_sorted);

        let run_length_stats = run_length_stats(&vec);
        assert_eq!(run_length_stats.run_count, 0);
        assert_eq!(run_length_stats.run_element_count, 0);
    }
}
