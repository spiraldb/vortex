use std::mem::size_of;

use const_for::const_for;
use num_traits::{PrimInt, Unsigned};
use seq_macro::seq;

use crate::{Pred, Satisfied};

/// BitPack into a compile-time known bit-width.
pub trait BitPack2<const W: usize>
where
    Self: Sized + Unsigned + PrimInt,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
{
    /// Packs 1024 elements into W bits each -> (1024 * W / 8) -> 128 * W bytes
    fn pack<'a>(input: &[Self; 1024], output: &'a mut [u8; 128 * W]);
}

impl<const W: usize> BitPack2<W> for u16
where
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
    [(); (1024 * W) / 16]:,
{
    // #[unroll_for_loops]
    fn pack<'a>(input: &[Self; 1024], raw_output: &'a mut [u8; 128 * W]) {
        const T: usize = size_of::<u16>() * 8;
        const WIDTH: usize = 3;
        const MASK: u16 = (1 << 3) - 1;
        const LANES: usize = 1024 / T;

        const ORDER: [u8; 8] = [0, 4, 2, 6, 1, 5, 3, 7];

        // ngates: we could fix output offsets and just write raw bytes?
        let output: &mut [Self; (1024 * WIDTH) / 16] = unsafe { std::mem::transmute(raw_output) };

        // First we loop over each lane in the virtual 1024 bit word.
        let mut src: u16;
        let mut tmp: u16 = 0;
        for i in 0..LANES {
            // Now we inline loop over each of the rows of the lane.

            tmp = 0;
            seq!(row in 0..16 {
                {
                src = input[LANES * row + i] & MASK;

                // Shift the src bits into their position in the tmp output variable.
                // if row == 0 {
                //    tmp = 0;
                //} else {
                tmp |= src << (row * WIDTH) % T;
                //}

                const curr_out: usize = (row * WIDTH) / T;
                const next_out: usize = ((row + 1) * WIDTH) / T;
                if next_out > curr_out {
                    output[LANES * curr_out + i] = tmp;

                    const remaining_bits: usize = ((row + 1) * WIDTH) % T;
                    tmp = src >> WIDTH - remaining_bits;
                }
                }
            });

            continue;

            // 0
            src = input[LANES * 0 + i] & MASK;
            tmp = src;

            // 1
            src = input[LANES * 1 + i] & MASK;
            tmp |= src << 3;

            // 2
            src = input[LANES * 2 + i] & MASK;
            tmp |= src << 6;

            // 3
            src = input[LANES * 3 + i] & MASK;
            tmp |= src << 9;

            // 4
            src = input[LANES * 4 + i] & MASK;
            tmp |= src << 12;

            // 5
            src = input[LANES * 5 + i] & MASK;
            tmp |= src << 15;

            println!("Writing {} to {}", tmp, LANES * 0 + i);
            output[LANES * 0 + i] = tmp;

            src = input[LANES * 5 + i] & MASK;
            tmp = src >> 1;

            // 6
            // 6 * W => 18, mod T == 2
            src = input[LANES * 6 + i] & MASK;
            tmp |= src << 2;

            // 7
            src = input[LANES * 7 + i] & MASK;
            tmp |= src << 5;

            src = input[LANES * 8 + i] & MASK;
            tmp |= src << 8;

            src = input[LANES * 9 + i] & MASK;
            tmp |= src << 11;

            src = input[LANES * 10 + i] & MASK;
            tmp |= src << 14;

            println!("Writing {} to {}", tmp, LANES * 1 + i);
            output[(LANES * 1) + i] = tmp;

            src = input[LANES * 10 + i] & MASK;
            tmp = src >> 2;

            // 11
            // 11 * W => 33, mod T == 1
            src = input[LANES * 11 + i] & MASK;
            tmp |= src << 1;

            src = input[LANES * 12 + i] & MASK;
            tmp |= src << 4;

            src = input[LANES * 13 + i] & MASK;
            tmp |= src << 7;

            src = input[LANES * 14 + i] & MASK;
            tmp |= src << 10;

            src = input[LANES * 15 + i] & MASK;
            tmp |= src << 13;

            println!("Writing {} to {}", tmp, LANES * 2 + i);
            output[(LANES * 2) + i] = tmp;
        }
    }
}

#[cfg(test)]
mod test {
    use std::mem::MaybeUninit;

    use super::*;
    use crate::TryBitPack;

    #[test]
    fn try_pack() {
        const WIDTH: usize = 3;
        let values = [3u16; 1024];
        let mut packed = [0u8; WIDTH * 128];
        BitPack2::<WIDTH>::pack(&values, &mut packed);

        let mut packed2 = [MaybeUninit::new(0u8); WIDTH * 128];
        let packed2 = &TryBitPack::try_pack(&values, WIDTH, &mut packed2).unwrap()[0..WIDTH * 128];

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
