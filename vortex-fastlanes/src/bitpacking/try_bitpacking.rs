use std::mem::size_of;

use arrayref::array_ref;
use fastlanes::BitPacking;
use seq_macro::seq;

struct UnsupportedBitWidth;

pub trait TryBitPacking: BitPacking {
    fn try_bitunpack_single(
        input: &[Self],
        width: usize,
        index: usize,
    ) -> Result<Self, UnsupportedBitWidth>;
}

macro_rules! impl_try_bitpacking {
    ($T:ty, $N:expr) => {
        impl TryBitPacking for $T {
            fn try_bitunpack_single(
                input: &[Self],
                width: usize,
                index: usize,
            ) -> Result<Self, UnsupportedBitWidth> {
                seq!(W in 0..$N {
                    if width == W {
                        const ELEMS: usize = 128 * W / size_of::<$T>();
                        Self::bitunpack_single::<W>(array_ref![input, 0, ELEMS], index);
                    }
                });
                Err(UnsupportedBitWidth)
            }
        }
    };
}

impl_try_bitpacking!(u8, 8);
impl_try_bitpacking!(u16, 16);
impl_try_bitpacking!(u32, 32);
impl_try_bitpacking!(u64, 64);
