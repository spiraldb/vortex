#![allow(incomplete_features)]
#![feature(generic_const_exprs)]
#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::mem::{size_of, MaybeUninit};

use arrayref::array_mut_ref;
use seq_macro::seq;
use uninit::prelude::VecCapacity;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub struct Pred<const B: bool>;

pub trait Satisfied {}

impl Satisfied for Pred<true> {}

/// BitPack into a compile-time known bit-width.
pub trait BitPack<const W: usize>
where
    Self: Sized,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
{
    fn bitpack<'a>(
        input: &[Self; 1024],
        output: &'a mut [MaybeUninit<u8>; 128 * W],
    ) -> &'a [u8; 128 * W];
}

/// BitUnpack from a compile-time known bit-width.
pub trait BitUnpack<const W: usize>
where
    Self: Sized,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
{
    fn bitunpack<'a>(
        input: &[u8; 128 * W],
        output: &'a mut [MaybeUninit<Self>; 1024],
    ) -> &'a [Self; 1024];
}

#[derive(Debug)]
pub struct UnsupportedBitWidth;

/// Try to bitpack into a runtime-known bit width.
pub trait TryBitPack
where
    Self: Sized,
{
    fn try_bitpack<'a>(
        input: &[Self; 1024],
        width: usize,
        output: &'a mut [MaybeUninit<u8>],
    ) -> Result<&'a [u8], UnsupportedBitWidth>;

    fn try_bitpack_into(
        input: &[Self; 1024],
        width: usize,
        output: &mut Vec<u8>,
    ) -> Result<(), UnsupportedBitWidth> {
        Self::try_bitpack(input, width, output.reserve_uninit(width * 128))?;
        unsafe { output.set_len(output.len() + (width * 128)) }
        Ok(())
    }
}

/// Try to bitunpack into a runtime-known bit width.
pub trait TryBitUnpack
where
    Self: Sized,
{
    fn try_bitunpack<'a>(
        input: &[u8],
        width: usize,
        output: &'a mut [MaybeUninit<Self>; 1024],
    ) -> Result<&'a [Self; 1024], UnsupportedBitWidth>;

    fn try_bitunpack_into(
        input: &[u8],
        width: usize,
        output: &mut Vec<Self>,
    ) -> Result<(), UnsupportedBitWidth> {
        Self::try_bitunpack(input, width, output.reserve_uninit(1024))?;
        unsafe { output.set_len(output.len() + 1024) }
        Ok(())
    }
}

macro_rules! bitpack_impl {
    ($T:ty, $W:literal) => {
        paste::item! {
            seq!(N in 1..$W {
                impl BitPack<N> for $T {
                    #[inline]
                    fn bitpack<'a>(
                        input: &[Self; 1024],
                        output: &'a mut [MaybeUninit<u8>; 128 * N],
                    ) -> &'a [u8; 128 * N] {
                            unsafe {
                                let output_array: &mut [u8; 128 * N] = std::mem::transmute(output);
                                [<fl_bitpack_ $T _u >]~N(input, output_array);
                                output_array
                            }
                    }
                }
            });
        }

        impl TryBitPack for $T {
            fn try_bitpack<'a>(
                input: &[Self; 1024],
                width: usize,
                output: &'a mut [MaybeUninit<u8>],
            ) -> Result<&'a [u8], UnsupportedBitWidth> {
                seq!(N in 1..$W {
                    match width {
                        #(N => Ok(BitPack::<N>::bitpack(input, array_mut_ref![output, 0, N * 128]).as_slice()),)*
                        _ => Err(UnsupportedBitWidth),
                    }
                })
            }
        }

    };
}

impl BitUnpack<4> for u16 {
    #[inline]
    fn bitunpack<'a>(
        input: &[u8; 128 * W],
        output: &'a mut [MaybeUninit<Self>; 1024],
    ) -> &'a [Self; 1024] {
        unsafe {
            let output_array: &mut [Self; 1024] = std::mem::transmute(output);
            fl_bitpack_u16_u4(input, output_array);
            output_array
        }
    }
}

impl TryBitPack for u16 {
    fn try_bitpack<'a>(
        input: &[Self; 1024],
        width: usize,
        output: &'a mut [MaybeUninit<u8>],
    ) -> Result<&'a [u8], UnsupportedBitWidth> {
        match width {
            4 => Ok(BitPack::<4>::bitpack(input, array_mut_ref![output, 0, 4 * 128]).as_slice()),
            _ => Err(UnsupportedBitWidth),
        }
    }
}

bitpack_impl!(u8, 8);
// bitpack_impl!(u16, 16);
bitpack_impl!(u32, 32);
bitpack_impl!(u64, 64);
