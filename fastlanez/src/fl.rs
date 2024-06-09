use std::mem::size_of;

use arrayref::array_mut_ref;
use num_traits::{PrimInt, Unsigned};
use paste::paste;
use seq_macro::seq;

use crate::{Pred, Satisfied, UnsupportedBitWidth};

pub const ORDER: [u8; 8] = [0, 4, 2, 6, 1, 5, 3, 7];

/// BitPack into a compile-time known bit-width.
pub trait BitPack2<const W: usize>
where
    Self: Sized + Unsigned + PrimInt,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
{
    const T: usize = size_of::<Self>() * 8;
    const LANES: usize = 1024 / Self::T;
    const WIDTH: usize = W;

    /// Packs 1024 elements into W bits each.
    /// The output is given as Self to ensure correct alignment.
    fn bitpack(input: &[Self; 1024], output: &mut [Self; 128 * W / size_of::<Self>()]);
}

// Macro for repeating a code block bit_size_of::<T> times.
macro_rules! seq_type_width {
    ($ident:ident in u8 $body:tt) => {seq!($ident in 0..8 $body);};
    ($ident:ident in u16 $body:tt) => {seq!($ident in 0..16 $body);};
    ($ident:ident in u32 $body:tt) => {seq!($ident in 0..32 $body);};
    ($ident:ident in u64 $body:tt) => {seq!($ident in 0..64 $body);};
}

// We need to use a macro instead of generic impl since we have to know the bit-width of T ahead
// of time.
macro_rules! impl_bitpacking {
    ($T:ty) => {
        paste! {
            impl<const W: usize> BitPack2<W> for $T
            where
                Pred<{ W > 0 }>: Satisfied,
                Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
                [(); 128 * W / size_of::<Self>()]:,
            {
                #[inline(never)]
                #[allow(unused_assignments)]
                fn bitpack<'a>(input: &[Self; 1024], output: &mut [Self; 128 * W / size_of::<Self>()]) {
                    let mask = ((1 << W) - 1);

                    // First we loop over each lane in the virtual 1024 bit word.
                    for i in 0..Self::LANES {
                        let mut tmp: $T = 0;

                        // Now we inline loop over each of the rows of the lane.
                        seq_type_width!(row in $T {{
                            let src = input[Self::LANES * row + i] & mask;

                            // Shift the src bits into their position in the tmp output variable.
                            if row == 0 {
                                tmp = src;
                            } else {
                                tmp |= src << (row * Self::WIDTH) % Self::T;
                            }

                            let curr_out_pos: usize = (row * Self::WIDTH) / Self::T;
                            let next_out_pos: usize = ((row + 1) * Self::WIDTH) / Self::T;
                            if next_out_pos > curr_out_pos {
                                output[Self::LANES * curr_out_pos + i] = tmp;

                                let remaining_bits: usize = ((row + 1) * Self::WIDTH) % Self::T;
                                tmp = src >> Self::WIDTH - remaining_bits;
                            }
                        }});
                    }
                }
            }
        }
    };
}

/// Try to bitpack into a runtime-known bit width.
pub trait TryBitPack2
where
    Self: Sized + Unsigned + PrimInt,
{
    fn try_pack(
        input: &[Self; 1024],
        width: usize,
        output: &mut [Self],
    ) -> Result<(), UnsupportedBitWidth>;
}

impl TryBitPack2 for u16 {
    fn try_pack(
        input: &[Self; 1024],
        width: usize,
        output: &mut [Self],
    ) -> Result<(), UnsupportedBitWidth> {
        seq!(W in 1..16 {
            match width {
                #(W => {
                    BitPack2::<W>::bitpack(input, array_mut_ref![output, 0, 128 * W / size_of::<u16>()]);
                    Ok(())
                })*,
                _ => Err(UnsupportedBitWidth),
            }
        })
    }
}

impl_bitpacking!(u8);
impl_bitpacking!(u16);
impl_bitpacking!(u32);
impl_bitpacking!(u64);

#[cfg(test)]
mod test {
    use std::mem::MaybeUninit;

    use super::*;

    #[test]
    fn try_pack() {
        const WIDTH: usize = 3;
        let values = [3u16; 1024];
        let mut packed = [0; 192];
        BitPack2::<WIDTH>::bitpack(&values, &mut packed);
        let packed: [u8; 384] = unsafe { std::mem::transmute(packed) };

        let mut packed2 = [MaybeUninit::new(0u8); WIDTH * 128];
        let packed2 = crate::bitpack::TryBitPack::try_pack(&values, WIDTH, &mut packed2).unwrap();

        println!("NEW: {:?}", &packed);
        println!("OLD: {:?}", &packed2);
        for i in 0..384 {
            if packed[i] != packed2[i] {
                panic!("Hmmm {}", i);
            }
        }
        assert_eq!(&packed, &packed2);
    }
}
