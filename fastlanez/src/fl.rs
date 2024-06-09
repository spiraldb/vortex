use std::mem::size_of;

use arrayref::array_mut_ref;
use num_traits::{PrimInt, Unsigned};
use paste::paste;
use seq_macro::seq;

use crate::{Pred, Satisfied, UnsupportedBitWidth};

pub const ORDER: [u8; 8] = [0, 4, 2, 6, 1, 5, 3, 7];

pub trait FastLanes: Sized + Unsigned + PrimInt {
    const T: usize = size_of::<Self>() * 8;
    const LANES: usize = 1024 / Self::T;
}

/// BitPack into a compile-time known bit-width.
pub trait BitPack2<const W: usize>
where
    Self: FastLanes,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
{
    const WIDTH: usize = W;

    /// Packs 1024 elements into W bits each.
    /// The output is given as Self to ensure correct alignment.
    fn bitpack(input: &[Self; 1024], output: &mut [Self; 128 * W / size_of::<Self>()]);

    /// Unpacks W-bit elements into 1024 elements.
    fn bitunpack(input: &[Self; 128 * W / size_of::<Self>()], output: &mut [Self; 1024]);
}

// Macro for repeating a code block bit_size_of::<T> times.
macro_rules! seq_type_width {
    ($ident:ident in u8 $body:tt) => {seq!($ident in 0..8 $body);};
    ($ident:ident in u16 $body:tt) => {seq!($ident in 0..16 $body);};
    ($ident:ident in u32 $body:tt) => {seq!($ident in 0..32 $body);};
    ($ident:ident in u64 $body:tt) => {seq!($ident in 0..64 $body);};
}

impl FastLanes for u16 {}

impl<const W: usize> BitPack2<W> for u16
where
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
    [(); 128 * W / size_of::<Self>()]:,
{
    fn bitpack(input: &[Self; 1024], output: &mut [Self; 128 * W / size_of::<Self>()]) {
        let mask = (1 << W) - 1;

        // First we loop over each lane in the virtual 1024 bit word.
        for i in 0..Self::LANES {
            let mut tmp: Self = 0;

            // Inlined loop over each of the rows of the lane.
            seq_type_width!(row in u16 {{
                let src = input[Self::LANES * row + i] & mask;

                // Shift the src bits into their position in the tmp output variable.
                if row == 0 {
                    tmp = src;
                } else {
                    tmp |= src << (row * Self::WIDTH) % Self::T;
                }

                // If the next input value overlaps with the next output, then we
                // write out the tmp variable and bring forward the remaining bits.
                let curr_out: usize = (row * Self::WIDTH) / Self::T;
                let next_out: usize = ((row + 1) * Self::WIDTH) / Self::T;
                if next_out > curr_out {
                    output[Self::LANES * curr_out + i] = tmp;

                    let remaining_bits: usize = ((row + 1) * Self::WIDTH) % Self::T;
                    tmp = src >> Self::WIDTH - remaining_bits;
                }
            }});
        }
    }

    fn bitunpack(input: &[Self; 128 * W / size_of::<u16>()], output: &mut [Self; 1024]) {
        let mut src: Self = 0;
        let mut tmp: Self = 0;
        let mut base: Self = 0;

        for i in 0..Self::LANES {
            src = input[i + 0];

            tmp = (src >> 0) & ((1 << 3) - 1);
            output[i + (Self::LANES * 0)] = tmp;

            tmp = (src >> 3) & ((1 << 3) - 1);
            output[i + (Self::LANES * 1)] = tmp;

            tmp = (src >> 6) & ((1 << 3) - 1);
            output[i + (Self::LANES * 2)] = tmp;

            tmp = (src >> 9) & ((1 << 3) - 1);
            output[i + (Self::LANES * 3)] = tmp;

            tmp = (src >> 12) & ((1 << 3) - 1);
            output[i + (Self::LANES * 4)] = tmp;

            tmp = (src >> 15) & ((1 << 1) - 1);

            let curr_in: usize = (1 * Self::WIDTH) / Self::T;
            src = input[i + 64];

            tmp |= ((src) & ((1 << 2) - 1)) << 1;
            output[i + (Self::LANES * 5)] = tmp;
            tmp = (src >> 2) & ((1 << 3) - 1);
            output[i + (Self::LANES * 6)] = tmp;
            tmp = (src >> 5) & ((1 << 3) - 1);
            output[i + (Self::LANES * 7)] = tmp;
            tmp = (src >> 8) & ((1 << 3) - 1);
            output[i + (Self::LANES * 8)] = tmp;
            tmp = (src >> 11) & ((1 << 3) - 1);
            output[i + (Self::LANES * 9)] = tmp;
            tmp = (src >> 14) & ((1 << 2) - 1);

            let curr_in: usize = (2 * Self::WIDTH) / Self::T;
            src = input[i + 128];
            tmp |= ((src) & ((1 << 1) - 1)) << 2;
            output[i + (Self::LANES * 10)] = tmp;
            tmp = (src >> 1) & ((1 << 3) - 1);
            output[i + (Self::LANES * 11)] = tmp;
            tmp = (src >> 4) & ((1 << 3) - 1);
            output[i + (Self::LANES * 12)] = tmp;
            tmp = (src >> 7) & ((1 << 3) - 1);
            output[i + (Self::LANES * 13)] = tmp;
            tmp = (src >> 10) & ((1 << 3) - 1);
            output[i + (Self::LANES * 14)] = tmp;
            tmp = (src >> 13) & ((1 << 3) - 1);
            output[i + (Self::LANES * 15)] = tmp;
        }
    }
}

// We need to use a macro instead of generic impl since we have to know the bit-width of T ahead
// of time.
macro_rules! impl_bitpacking {
    ($T:ty) => {
        paste! {
            impl FastLanes for $T {}


            impl<const W: usize> BitPack2<W> for $T
            where
                Pred<{ W > 0 }>: Satisfied,
                Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
                [(); 128 * W / size_of::<Self>()]:,
            {
                fn bitunpack(input: &[Self; 128 * W / size_of::<Self>()], output: &mut [Self; 1024]) {
                    todo!()
                }

                #[inline(never)] // Makes it easier to disassemble and validate ASM.
                #[allow(unused_assignments)] // Inlined loop gives unused assignment on final iteration
                fn bitpack<'a>(input: &[Self; 1024], output: &mut [Self; 128 * W / size_of::<Self>()]) {
                    let mask = ((1 << W) - 1);

                    // First we loop over each lane in the virtual 1024 bit word.
                    for i in 0..Self::LANES {
                        let mut tmp: $T = 0;

                        // Inlined loop over each of the rows of the lane.
                        seq_type_width!(row in $T {{
                            let src = input[Self::LANES * row + i] & mask;

                            // Shift the src bits into their position in the tmp output variable.
                            if row == 0 {
                                tmp = src;
                            } else {
                                tmp |= src << (row * Self::WIDTH) % Self::T;
                            }

                            // If the next input value overlaps with the next output, then we
                            // write out the tmp variable and bring forward the remaining bits.
                            let curr_out: usize = (row * Self::WIDTH) / Self::T;
                            let next_out: usize = ((row + 1) * Self::WIDTH) / Self::T;
                            if next_out > curr_out {
                                output[Self::LANES * curr_out + i] = tmp;

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
// impl_bitpacking!(u16);
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

    #[test]
    fn try_unpack() {
        const WIDTH: usize = 3;

        let values = [3u16; 1024];
        let mut packed = [0; 192];
        BitPack2::<WIDTH>::bitpack(&values, &mut packed);

        let mut unpacked = [0; 1024];
        BitPack2::<WIDTH>::bitunpack(&packed, &mut unpacked);

        println!("Unpacked: {:?}", &unpacked);
        assert_eq!(&unpacked, &values);
    }
}
