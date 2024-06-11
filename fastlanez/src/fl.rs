use std::mem::size_of;

use num_traits::{One, PrimInt, Unsigned};
use paste::paste;
use seq_macro::seq;

use crate::{Pred, Satisfied};

pub const ORDER: [u8; 8] = [0, 4, 2, 6, 1, 5, 3, 7];

pub trait FastLanes: Sized + Unsigned + PrimInt {
    const T: usize = size_of::<Self>() * 8;
    const LANES: usize = 1024 / Self::T;
}

pub struct BitPackWidth<const W: usize>;
pub trait SupportedBitPackWidth<T> {}
impl<const W: usize, T> SupportedBitPackWidth<T> for BitPackWidth<W>
where
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<T>() }>: Satisfied,
{
}

/// BitPack into a compile-time known bit-width.
pub trait BitPack2: FastLanes {
    /// Packs 1024 elements into W bits each.
    /// The output is given as Self to ensure correct alignment.
    fn bitpack<const W: usize>(
        input: &[Self; 1024],
        output: &mut [Self; 128 * W / size_of::<Self>()],
    ) where
        BitPackWidth<W>: SupportedBitPackWidth<Self>;

    /// Unpacks W-bit elements into 1024 elements.
    fn bitunpack<const W: usize>(
        input: &[Self; 128 * W / size_of::<Self>()],
        output: &mut [Self; 1024],
    ) where
        BitPackWidth<W>: SupportedBitPackWidth<Self>;

    fn bitunpack_single<const W: usize>(
        input: &[Self; 128 * W / size_of::<Self>()],
        index: usize,
    ) -> Self
    where
        BitPackWidth<W>: SupportedBitPackWidth<Self>;
}

// Macro for repeating a code block bit_size_of::<T> times.
macro_rules! seq_type_width {
    ($ident:ident in u8 $body:tt) => {seq!($ident in 0..8 $body);};
    ($ident:ident in u16 $body:tt) => {seq!($ident in 0..16 $body);};
    ($ident:ident in u32 $body:tt) => {seq!($ident in 0..32 $body);};
    ($ident:ident in u64 $body:tt) => {seq!($ident in 0..64 $body);};
}

#[inline]
fn mask<T: PrimInt + Unsigned + One>(width: usize) -> T {
    (T::one() << width) - T::one()
}

// We need to use a macro instead of generic impl since we have to know the bit-width of T ahead
// of time.
macro_rules! impl_bitpacking {
    ($T:ty) => {
        paste! {
            impl FastLanes for $T {}

            impl BitPack2 for $T {
                #[inline(never)] // Makes it easier to disassemble and validate ASM.
                #[allow(unused_assignments)] // Inlined loop gives unused assignment on final iteration
                fn bitpack<const W: usize>(
                    input: &[Self; 1024],
                    output: &mut [Self; 128 * W / size_of::<Self>()],
                ) where BitPackWidth<W>: SupportedBitPackWidth<Self> {
                    let mask = (1 << W) - 1;

                    // First we loop over each lane in the virtual 1024 bit word.
                    for i in 0..Self::LANES {
                        let mut tmp: Self = 0;

                        // Loop over each of the rows of the lane.
                        // Inlining this loop means all branches are known at compile time and
                        // the code is auto-vectorized for SIMD execution.
                        seq_type_width!(row in $T {{
                            let src = input[Self::LANES * row + i] & mask;

                            // Shift the src bits into their position in the tmp output variable.
                            if row == 0 {
                                tmp = src;
                            } else {
                                tmp |= src << (row * W) % Self::T;
                            }

                            // If the next input value overlaps with the next output, then we
                            // write out the tmp variable and bring forward the remaining bits.
                            let curr_pos: usize = (row * W) / Self::T;
                            let next_pos: usize = ((row + 1) * W) / Self::T;
                            if next_pos > curr_pos {
                                output[Self::LANES * curr_pos + i] = tmp;

                                let remaining_bits: usize = ((row + 1) * W) % Self::T;
                                tmp = src >> W - remaining_bits;
                            }
                        }});
                    }
                }

                #[inline(never)]
                fn bitunpack<const W: usize>(
                    input: &[Self; 128 * W / size_of::<Self>()],
                    output: &mut [Self; 1024],
                ) where BitPackWidth<W>: SupportedBitPackWidth<Self> {
                    for i in 0..Self::LANES {
                        let mut src = input[i];
                        let mut tmp: Self;

                        seq_type_width!(row in $T {{
                            let curr_pos: usize = (row * W) / Self::T;
                            let next_pos = ((row + 1) * W) / Self::T;

                            let shift = (row * W) % Self::T;

                            if next_pos > curr_pos {
                                // Consume some bits from the curr input, the remainder are in the next input
                                let remaining_bits = ((row + 1) * W) % Self::T;
                                let current_bits = W - remaining_bits;
                                tmp = (src >> shift) & mask::<Self>(current_bits);

                                if next_pos < W {
                                    // Load the next input value
                                    src = input[Self::LANES * next_pos + i];
                                    // Consume the remaining bits from the next input value.
                                    tmp |= (src & mask::<Self>(remaining_bits)) << current_bits;
                                }
                            } else {
                                // Otherwise, just grab W bits from the src value
                                tmp = (src >> shift) & mask::<Self>(W);
                            }

                            // Write out the unpacked value
                            output[(Self::LANES * row) + i] = tmp;
                        }});
                    }
                }

                #[inline(never)]
                fn bitunpack_single<const W: usize>(
                    input: &[Self; 128 * W / size_of::<Self>()],
                    index: usize,
                ) -> Self where BitPackWidth<W>: SupportedBitPackWidth<Self> {
                    let lane_index = index % Self::LANES;
                    let lane_start_bit = (index / Self::LANES) * W;

                    let (lsb, msb) = {
                        // the value may be split across two words
                        let lane_start_word = lane_start_bit / Self::T;
                        let lane_end_word = (lane_start_bit + W - 1) / Self::T;

                        (
                            input[lane_start_word * Self::LANES + lane_index],
                            input[lane_end_word * Self::LANES + lane_index], // this may be a duplicate
                        )
                    };

                    let shift = lane_start_bit % Self::T;
                    if shift == 0 {
                        (lsb >> shift) & mask::<Self>(W)
                    } else {
                        // If shift == 0, then this shift overflows, instead of shifting to zero.
                        // This forces us to introduce a branch. Any way to avoid?
                        let hi = msb << (Self::T - shift);
                        let lo = lsb >> shift;
                        (lo | hi) & mask::<Self>(W)
                    }
                }
            }
        }
    };
}

impl_bitpacking!(u8);
impl_bitpacking!(u16);
impl_bitpacking!(u32);
impl_bitpacking!(u64);

#[cfg(test)]
// #[cfg(not(debug_assertions))] // Only run in release mode
mod test {
    use super::*;

    macro_rules! test_round_trip {
        ($T:ty, $W:literal) => {
            paste! {
                #[test]
                fn [<try_round_trip_ $T _ $W>]() {
                    let mut values: [$T; 1024] = [0; 1024];
                    for i in 0..1024 {
                        values[i] = (i % (1 << $W)) as $T;
                    }

                    let mut packed = [0; 128 * $W / size_of::<$T>()];
                    BitPack2::bitpack::<$W>(&values, &mut packed);

                    let mut unpacked = [0; 1024];
                    BitPack2::bitunpack::<$W>(&packed, &mut unpacked);

                    assert_eq!(&unpacked, &values);
                }

                #[test]
                fn [<try_unpack_single_ $T _ $W>]() {
                    let mut values: [$T; 1024] = [0; 1024];
                    for i in 0..1024 {
                        values[i] = (i % (1 << $W)) as $T;
                    }

                    let mut packed = [0; 128 * $W / size_of::<$T>()];
                    BitPack2::bitpack::<$W>(&values, &mut packed);

                    for (idx, value) in values.into_iter().enumerate() {
                        assert_eq!(BitPack2::bitunpack_single::<$W>(&packed, idx), value);
                    }
                }
            }
        };
    }

    seq!(W in 1..8 { test_round_trip!(u8, W); });
    seq!(W in 1..16 { test_round_trip!(u16, W); });
    seq!(W in 1..32 { test_round_trip!(u32, W); });
    seq!(W in 1..64 { test_round_trip!(u64, W); });
}
