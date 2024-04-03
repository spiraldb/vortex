use std::mem::size_of;

use arrayref::array_mut_ref;
use fastlanez_sys::*;
use uninit::prelude::VecCapacity;

pub trait Delta
where
    Self: Sized + Copy + Default,
{
    /// input is assumed to already be in the transposed layout
    /// call transpose() to convert from the original layout
    fn encode_transposed(
        input: &[Self; 1024],
        base: &mut [Self; 128 / size_of::<Self>()],
        output: &mut Vec<Self>,
    );

    /// output is still in the transposed layout
    /// call untranspose() to put it back in the original layout
    fn decode_transposed(
        input: &[Self; 1024],
        base: &mut [Self; 128 / size_of::<Self>()],
        output: &mut [Self; 1024],
    );

    fn lanes() -> usize {
        // fastlanez processes 1024 bits (128 bytes) at a time
        128 / std::mem::size_of::<Self>()
    }
}

macro_rules! delta_impl {
    ($T:ty) => {
        paste::item! {
            impl Delta for $T {
                fn encode_transposed(
                    input: &[Self; 1024],
                    base: &mut [Self; 128 / size_of::<Self>()],
                    output: &mut Vec<Self>,
                ) {
                    unsafe {
                        [<fl_delta_encode_ $T>](
                            input,
                            base,
                            array_mut_ref![output.reserve_uninit(1024), 0, 1024] as *mut [std::mem::MaybeUninit<Self>; 1024] as *mut [Self; 1024],
                        );
                        output.set_len(output.len() + 1024);
                    }
                }

                fn decode_transposed(
                    input: &[Self; 1024],
                    base: &mut [Self; 128 / size_of::<Self>()],
                    output: &mut [Self; 1024],
                ) {
                    unsafe { [<fl_delta_decode_ $T>](input, base, output); }
                }
            }
        }
    };
}

delta_impl!(i8);
delta_impl!(i16);
delta_impl!(i32);
delta_impl!(i64);
delta_impl!(u8);
delta_impl!(u16);
delta_impl!(u32);
delta_impl!(u64);
