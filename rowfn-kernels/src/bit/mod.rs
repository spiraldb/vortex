// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Borrowed bitmap access and little-endian Boolean packing.
//!
//! Views accept unaligned, unpadded byte slices. Packing writes caller-owned words.

mod view;
pub use view::Bitmap;
pub use view::BitmapView;

pub mod pack;
pub use pack::*;

/// Packs up to 64 boolean values into a little-endian `u64` word.
///
/// This is [`collect_bool_words`] for a single word: a full 64-bit word is materialized as a
/// `[bool; 64]` and packed with the baseline SIMD byte→bit instruction of the target; shorter
/// lengths fall back to the bit-at-a-time [`collect_bool_word_scalar`] loop.
#[inline]
pub fn collect_bool_word<F>(len: usize, f: F) -> u64
where
    F: FnMut(usize) -> bool,
{
    assert!(len <= 64, "cannot pack {len} bits into a u64 word");

    let mut word = [0u64; 1];
    collect_bool_words_inline(&mut word, len, f);
    word[0]
}

/// Pack `len` boolean values returned by `f` into the prefix of `words`, LSB-first,
/// 64 bits per `u64`. `words` must have capacity for at least `len.div_ceil(64)` entries.
///
/// `f` is invoked exactly once per index, in ascending order `0..len`.
///
/// Writes via `=` (not `|=`), so the destination need not be zero-initialised.
///
/// The word loop packs with the baseline SIMD kernel of the target (SSE2 on x86-64, NEON on
/// aarch64), which inlines fully into the caller together with the predicate and the
/// `[bool; 64]` materialization, wider kernels would sit behind a non-inlinable
/// `#[target_feature]` boundary that deoptimizes expensive predicates. See
/// the packing module for the performance constraints on `f`.
///
/// Prefer this entry point for every predicate; only switch to
/// [`collect_bool_words_multiversioned`] after carefully checking that your specific `f`
/// meets its contract.
#[inline]
pub fn collect_bool_words<F>(words: &mut [u64], len: usize, f: F)
where
    F: FnMut(usize) -> bool,
{
    let num_words = len.div_ceil(64);
    assert!(
        words.len() >= num_words,
        "words slice has {} entries, need at least {num_words}",
        words.len(),
    );

    collect_bool_words_inline(words, len, f)
}

/// Read up to 8 bytes as a little-endian `u64`, zero-padding the high bytes when fewer than 8
/// bytes are supplied.
///
/// This preserves least-significant-bit-first bitmap numbering on little- and big-endian
/// targets. For a full 8-byte slice it lowers to a single word load.
#[inline]
pub fn read_u64_le(bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() <= 8);
    let mut buf = [0u8; 8];
    buf[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(buf)
}

/// Splice a packed word `w` (whose bits above the highest valid bit are zero) into
/// `words` at the given bit position.
///
/// The destination word at `bit_offset / 64` is OR'd, preserving any bits below
/// `bit_offset % 64`. When `w` has high bits that spill into the next word, those
/// bits are *assigned* (not OR'd), so callers must ensure that next slot is zero
/// (for example, by zero-initializing the destination).
///
/// `words.len()` need only cover the slots `w` actually writes to: skipping the
/// spillover when its bits are all zero means a tail that fits entirely in the
/// leading word never touches `words[dest_word + 1]`.
#[inline]
pub fn splice_word_at_bit(words: &mut [u64], bit_offset: usize, word: u64) {
    let dest_word = bit_offset / 64;
    let bit_in_word = bit_offset % 64;
    words[dest_word] |= word << bit_in_word;
    if bit_in_word != 0 {
        let high = word >> (64 - bit_in_word);
        if high != 0 {
            words[dest_word + 1] = high;
        }
    }
}

/// Pack `len` boolean values returned by `f` into `words` starting at bit position
/// `bit_offset`, LSB-first.
///
/// Composes [`collect_bool_word`] (pack up to 64 bools into a u64) with
/// [`splice_word_at_bit`] (merge the packed word into the destination via shift-OR).
///
/// `words` must have at least `(bit_offset + len).div_ceil(64)` entries; see
/// [`splice_word_at_bit`] for zero-init requirements on words above the cursor.
#[inline]
pub fn pack_bools_into_words<F>(words: &mut [u64], bit_offset: usize, len: usize, mut f: F)
where
    F: FnMut(usize) -> bool,
{
    if len == 0 {
        return;
    }
    let num_words = (bit_offset + len).div_ceil(64);
    assert!(
        words.len() >= num_words,
        "words slice has {} entries, need at least {num_words}",
        words.len(),
    );

    let mut done = 0;
    while len - done >= 64 {
        let word = collect_bool_word(64, |bit| f(done + bit));
        splice_word_at_bit(words, bit_offset + done, word);
        done += 64;
    }
    let tail = len - done;
    if tail > 0 {
        let word = collect_bool_word(tail, |bit| f(done + bit));
        splice_word_at_bit(words, bit_offset + done, word);
    }
}

