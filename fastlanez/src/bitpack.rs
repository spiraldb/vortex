use std::mem::{size_of, MaybeUninit};

use arrayref::{array_mut_ref, array_ref};
use fastlanez_sys::*;
use num_traits::{PrimInt, Unsigned};
use seq_macro::seq;
use uninit::prelude::VecCapacity;

use crate::{Pred, Satisfied};

/// BitPack into a compile-time known bit-width.
pub trait BitPack<const W: usize>
where
    Self: Sized,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
{
    fn pack<'a>(
        input: &[Self; 1024],
        output: &'a mut [MaybeUninit<u8>; 128 * W],
    ) -> &'a [u8; 128 * W];

    fn unpack<'a>(
        input: &[u8; 128 * W],
        output: &'a mut [MaybeUninit<Self>; 1024],
    ) -> &'a [Self; 1024];
}

#[derive(Debug)]
pub struct UnsupportedBitWidth;

/// Try to bitpack into a runtime-known bit width.
pub trait TryBitPack
where
    Self: Sized + Unsigned + PrimInt,
{
    fn try_pack<'a>(
        input: &[Self; 1024],
        width: usize,
        output: &'a mut [MaybeUninit<u8>],
    ) -> Result<&'a [u8], UnsupportedBitWidth>;

    fn try_pack_into(
        input: &[Self; 1024],
        width: usize,
        output: &mut Vec<u8>,
    ) -> Result<(), UnsupportedBitWidth> {
        Self::try_pack(input, width, output.reserve_uninit(width * 128))?;
        unsafe { output.set_len(output.len() + (width * 128)) }
        Ok(())
    }

    fn try_unpack<'a>(
        input: &[u8],
        width: usize,
        output: &'a mut [MaybeUninit<Self>; 1024],
    ) -> Result<&'a [Self; 1024], UnsupportedBitWidth>;

    fn try_unpack_into(
        input: &[u8],
        width: usize,
        output: &mut Vec<Self>,
    ) -> Result<(), UnsupportedBitWidth> {
        Self::try_unpack(
            input,
            width,
            array_mut_ref![output.reserve_uninit(1024), 0, 1024],
        )?;
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
                    fn pack<'a>(
                        input: &[Self; 1024],
                        output: &'a mut [MaybeUninit<u8>; 128 * N],
                    ) -> &'a [u8; 128 * N] {
                            unsafe {
                                let output_array: &mut [u8; 128 * N] = std::mem::transmute(output);
                                [<fl_bitpack_ $T _u >]~N(input, output_array);
                                output_array
                            }
                    }

                    #[inline]
                    fn unpack<'a>(
                        input: &[u8; 128 * N],
                        output: &'a mut [MaybeUninit<Self>; 1024],
                    ) -> &'a [Self; 1024] {
                        unsafe {
                            let output_array: &mut [Self; 1024] = std::mem::transmute(output);
                            [<fl_bitunpack_ $T _u >]~N(input, output_array);
                            output_array
                        }
                    }
                }
            });
        }

        impl TryBitPack for $T {
            fn try_pack<'a>(
                input: &[Self; 1024],
                width: usize,
                output: &'a mut [MaybeUninit<u8>],
            ) -> Result<&'a [u8], UnsupportedBitWidth> {
                seq!(N in 1..$W {
                    match width {
                        #(N => Ok(BitPack::<N>::pack(input, array_mut_ref![output, 0, N * 128]).as_slice()),)*
                        _ => Err(UnsupportedBitWidth),
                    }
                })
            }

            fn try_unpack<'a>(
                input: &[u8],
                width: usize,
                output: &'a mut [MaybeUninit<Self>; 1024],
            ) -> Result<&'a [Self; 1024], UnsupportedBitWidth> {
                seq!(N in 1..$W {
                    match width {
                        #(N => Ok(BitPack::<N>::unpack(array_ref![input, 0, N * 128], output)),)*
                        _ => Err(UnsupportedBitWidth),
                    }
                })
            }
        }
    };
}

bitpack_impl!(u8, 8);
bitpack_impl!(u16, 16);
bitpack_impl!(u32, 32);
bitpack_impl!(u64, 64);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_bitpack_roundtrip() {
        let input = (0u32..1024).collect::<Vec<_>>();
        let mut output = Vec::new();
        TryBitPack::try_pack_into(array_ref![input, 0, 1024], 10, &mut output).unwrap();
        assert_eq!(output.len(), 1280);

        let mut decoded: Vec<u32> = Vec::new();
        TryBitPack::try_unpack_into(&output, 10, &mut decoded).unwrap();
        assert_eq!(input, decoded);
    }
}
