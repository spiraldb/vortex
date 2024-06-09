use std::mem::size_of;
use std::ops::BitOrAssign;

use arrayref::array_mut_ref;
use num_traits::{One, PrimInt, Unsigned, Zero};
use seq_macro::seq;

use crate::{Pred, Satisfied, UnsupportedBitWidth};

/// BitPack into a compile-time known bit-width.
pub trait BitPack2<const W: usize>
where
    Self: Sized + Unsigned + PrimInt + One,
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
{
    const T: usize = size_of::<Self>() * 8;
    const LANES: usize = 1024 / Self::T;
    const WIDTH: usize = W;

    /// Packs 1024 elements into W bits each -> (1024 * W / 8) -> 128 * W bytes
    fn bitpacker<'a>(input: &[Self; 1024], output: &'a mut [u8; 128 * W]);

    #[inline]
    fn mask() -> Self {
        (Self::one() << Self::WIDTH) - Self::one()
    }
}

impl<const W: usize, T: PrimInt + Unsigned + Zero + BitOrAssign> BitPack2<W> for T
where
    Pred<{ W > 0 }>: Satisfied,
    Pred<{ W < 8 * size_of::<Self>() }>: Satisfied,
    [(); 128 * W / size_of::<Self>()]:,
{
    fn bitpacker<'a>(input: &[Self; 1024], output_bytes: &'a mut [u8; 128 * W]) {
        const ORDER: [u8; 8] = [0, 4, 2, 6, 1, 5, 3, 7];

        let output_ints: &mut [Self; 128 * W / size_of::<Self>()] =
            unsafe { std::mem::transmute(output_bytes) };

        // First we loop over each lane in the virtual 1024 bit word.
        let mut src: T;
        let mut tmp: T;
        for i in 0..Self::LANES {
            // Now we inline loop over each of the rows of the lane.

            tmp = T::zero();
            for row in 0..Self::T {
                src = input[Self::LANES * row + i] & Self::mask();

                // Shift the src bits into their position in the tmp output variable.
                tmp |= src << (row * Self::WIDTH) % Self::T;

                let curr_out: usize = (row * Self::WIDTH) / Self::T;
                let next_out: usize = ((row + 1) * Self::WIDTH) / Self::T;
                if next_out > curr_out {
                    output_ints[Self::LANES * curr_out + i] = tmp;

                    let remaining_bits: usize = ((row + 1) * Self::WIDTH) % Self::T;
                    tmp = src >> Self::WIDTH - remaining_bits;
                }
            }
        }
    }
}

/// Try to bitpack into a runtime-known bit width.
pub trait TryBitPack
where
    Self: Sized + Unsigned + PrimInt,
{
    fn try_pack<'a>(
        input: &[Self; 1024],
        width: usize,
        output: &'a mut [u8],
    ) -> Result<(), UnsupportedBitWidth>;
}

impl TryBitPack for u16 {
    fn try_pack<'a>(
        input: &[Self; 1024],
        width: usize,
        output: &'a mut [u8],
    ) -> Result<(), UnsupportedBitWidth> {
        seq!(W in 1..16 {
            match width {
                #(W => {
                    BitPack2::<W>::bitpacker(input, array_mut_ref![output, 0, 128 * W]);
                    Ok(())
                })*,
                _ => Err(UnsupportedBitWidth),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use std::mem::MaybeUninit;

    use super::*;

    #[test]
    fn try_pack() {
        const WIDTH: usize = 3;
        let values = [3u16; 1024];
        let mut packed = [0; 384];
        BitPack2::<WIDTH>::bitpacker(&values, &mut packed);

        let mut packed2 = [MaybeUninit::new(0u8); WIDTH * 128];
        let packed2 = &crate::bitpack::TryBitPack::try_pack(&values, WIDTH, &mut packed2).unwrap()
            [0..WIDTH * 128];

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
