// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Packed bitmaps that can be used to store boolean values.
//!
//! This module provides a wrapper on top of the `Buffer` type to store mutable and immutable
//! bitsets. The bitsets are stored in little-endian order, meaning that the least significant bit
//! of the first byte is the first bit in the bitset.
#[cfg(feature = "arrow")]
mod arrow;
mod buf;
mod buf_mut;
mod count_ones;
mod macros;
mod meta;
mod ops;
mod pack;
mod select;
mod view;

pub use arrow_buffer::bit_chunk_iterator::BitChunkIterator;
pub use arrow_buffer::bit_chunk_iterator::BitChunks;
pub use arrow_buffer::bit_chunk_iterator::UnalignedBitChunk;
pub use arrow_buffer::bit_chunk_iterator::UnalignedBitChunkIterator;
pub use arrow_buffer::bit_iterator::BitIndexIterator;
pub use arrow_buffer::bit_iterator::BitIterator;
pub use arrow_buffer::bit_iterator::BitSliceIterator;
pub use buf::*;
pub use buf_mut::*;
pub use meta::*;
pub use pack::*;
pub use view::*;

pub use rowfn_kernels::bit::{collect_bool_word, collect_bool_words, read_u64_le, splice_word_at_bit, pack_bools_into_words};

/// Get the bit value at `index` out of `buf`.
///
/// # Panics
///
/// Panics if `index` is not between 0 and length of `buf * 8`.
#[allow(clippy::inline_always)]
#[inline(always)]
pub fn get_bit(buf: &[u8], index: usize) -> bool {
    buf[index / 8] & (1 << (index % 8)) != 0
}

/// Get the bit value at `index` out of `buf` without bounds checking.
///
/// # Safety
///
/// `index` must be between 0 and length of `buf * 8`.
#[allow(clippy::inline_always)]
#[inline(always)]
pub unsafe fn get_bit_unchecked(buf: *const u8, index: usize) -> bool {
    (unsafe { *buf.add(index / 8) } & (1 << (index % 8))) != 0
}

/// Set the bit value at `index` in `buf` without bounds checking.
///
/// # Safety
///
/// `index` must be between 0 and length of `buf * 8`.
#[allow(clippy::inline_always)]
#[inline(always)]
pub unsafe fn set_bit_unchecked(buf: *mut u8, index: usize) {
    unsafe { *buf.add(index / 8) |= 1 << (index % 8) };
}

/// Unset the bit value at `index` in `buf` without bounds checking.
///
/// # Safety
///
/// `index` must be between 0 and length of `buf * 8`.
#[allow(clippy::inline_always)]
#[inline(always)]
pub unsafe fn unset_bit_unchecked(buf: *mut u8, index: usize) {
    unsafe { *buf.add(index / 8) &= !(1 << (index % 8)) };
}

#[cfg(test)]
mod tests {
    use super::collect_bool_word;
    use super::pack_bools_into_words;
    use super::read_u64_le;

    #[test]
    fn collect_bool_word_packs_lsb_first() {
        let word = collect_bool_word(5, |idx| idx.is_multiple_of(2));
        assert_eq!(word, 0b10101);
    }

    #[test]
    fn collect_bool_word_empty() {
        assert_eq!(collect_bool_word(0, |_| true), 0);
    }

    #[test]
    fn read_u64_le_zero_pads_tail() {
        assert_eq!(read_u64_le(&[0x34, 0x12]), 0x1234);
        assert_eq!(read_u64_le(&[0xff; 8]), u64::MAX);
    }

    #[test]
    #[should_panic(expected = "cannot pack 65 bits into a u64 word")]
    fn collect_bool_word_rejects_too_many_bits() {
        let _ = collect_bool_word(65, |_| true);
    }

    fn pack(bit_offset: usize, len: usize, f: impl Fn(usize) -> bool) -> Vec<bool> {
        let num_words = (bit_offset + len).div_ceil(64);
        let mut words = vec![0u64; num_words];
        pack_bools_into_words(&mut words, bit_offset, len, &f);
        (0..bit_offset + len)
            .map(|i| (words[i / 64] >> (i % 64)) & 1 == 1)
            .collect()
    }

    #[test]
    fn pack_bools_aligned_multi_word_with_tail() {
        let bits = pack(0, 130, |i| i.is_multiple_of(3));
        for i in 0..130 {
            assert_eq!(bits[i], i.is_multiple_of(3), "bit {i}");
        }
    }

    #[test]
    fn pack_bools_unaligned_crossing_words() {
        let bits = pack(40, 200, |i| i.is_multiple_of(7));
        assert!(bits[..40].iter().all(|&b| !b));
        for i in 0..200 {
            assert_eq!(bits[40 + i], i.is_multiple_of(7), "bit {}", 40 + i);
        }
    }

    #[test]
    fn pack_bools_preserves_low_bits_of_leading_word() {
        let mut words = vec![0u64; 2];
        words[0] = 0b11111;
        pack_bools_into_words(&mut words, 5, 70, |_| true);
        for i in 0..5 {
            assert_eq!((words[0] >> i) & 1, 1, "preserved bit {i}");
        }
        for i in 5..75 {
            assert_eq!((words[i / 64] >> (i % 64)) & 1, 1, "extended bit {i}");
        }
    }
}
