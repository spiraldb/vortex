// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Population count over a bit range.
//!
//! The kernels are one plain word loop; LLVM vectorizes it according to the target features
//! enabled on the enclosing function. On x86_64 the loop is compiled once per feature set and
//! selected at runtime, so the count always uses `vpopcntq` (AVX-512 VPOPCNTDQ), a `vpshufb`
//! nibble lookup (AVX2), or `popcnt`, even in builds whose baseline target lacks them. Other
//! architectures use the portable loop, whose baseline already has a vector popcount (NEON
//! `cnt` on aarch64).

#[cfg(target_arch = "x86_64")]
use crate::dispatch::CpuKernel;

/// Below this many bits, a build with compile-time POPCNT counts inline rather than paying
/// for the indirect call to a vectorized kernel.
#[cfg(target_arch = "x86_64")]
const DISPATCH_MIN_BITS: usize = 256;

#[inline]
pub fn count_ones(bytes: &[u8], offset: usize, len: usize) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if !cfg!(target_feature = "popcnt") || len >= DISPATCH_MIN_BITS {
            return count_ones_x86(bytes, offset, len);
        }
    }
    count_ones_portable(bytes, offset, len)
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn count_ones_x86(bytes: &[u8], offset: usize, len: usize) -> usize {
    type CountOnes = unsafe fn(&[u8], usize, usize) -> usize;
    static KERNEL: CpuKernel<CountOnes> = CpuKernel::new(|| {
        if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512vpopcntdq") {
            return count_ones_avx512;
        }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("popcnt") {
            return count_ones_avx2;
        }
        if is_x86_feature_detected!("popcnt") {
            return count_ones_popcnt;
        }
        count_ones_portable
    });
    // SAFETY: the selector only returns kernels that are safe or whose required CPU
    // features were probed before selection.
    unsafe { KERNEL.get()(bytes, offset, len) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx512vpopcntdq,popcnt")]
unsafe fn count_ones_avx512(bytes: &[u8], offset: usize, len: usize) -> usize {
    count_ones_portable(bytes, offset, len)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,popcnt")]
unsafe fn count_ones_avx2(bytes: &[u8], offset: usize, len: usize) -> usize {
    count_ones_portable(bytes, offset, len)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "popcnt")]
unsafe fn count_ones_popcnt(bytes: &[u8], offset: usize, len: usize) -> usize {
    count_ones_portable(bytes, offset, len)
}

/// Always inlined so each `#[target_feature]` kernel compiles its own copy with its features.
#[inline(always)]
fn count_ones_portable(bytes: &[u8], offset: usize, len: usize) -> usize {
    if bytes.is_empty() {
        return 0;
    }

    let (head, middle, tail) = align_offset_len(bytes, offset, len);

    let (words, tail_bytes) = middle.as_chunks::<8>();
    let middle_count = words
        .iter()
        .map(|word| u64::from_le_bytes(*word).count_ones() as usize)
        .sum::<usize>()
        + tail_bytes
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum::<usize>();

    head.map_or(0, |v| v.count_ones() as usize)
        + middle_count
        + tail.map_or(0, |v| v.count_ones() as usize)
}

#[inline]
pub(super) fn align_offset_len(
    bytes: &[u8],
    offset: usize,
    len: usize,
) -> (Option<u8>, &[u8], Option<u8>) {
    let start_byte = offset / 8;
    let start_bit = offset % 8;
    let end_bit = offset + len;
    let end_byte = end_bit / 8;
    let head = (start_bit != 0).then(|| {
        let start_len = (8 - start_bit).min(len);
        mask_byte(bytes[start_byte], start_bit, start_len)
    });

    let middle_start = start_byte + usize::from(start_bit != 0);
    let middle_end = end_byte;
    let middle = if middle_start < middle_end {
        &bytes[middle_start..middle_end]
    } else {
        &[]
    };

    let consumed = if start_bit != 0 {
        (8 - start_bit).min(len)
    } else {
        0
    } + middle.len() * 8;
    let tail_len = len - consumed;
    let tail = (tail_len != 0).then(|| mask_byte(bytes[middle_end], 0, tail_len));

    (head, middle, tail)
}

#[inline]
fn mask_byte(byte: u8, bit_offset: usize, bit_len: usize) -> u8 {
    debug_assert!(bit_offset < 8);
    debug_assert!(bit_len <= 8 - bit_offset);

    let shifted = byte >> bit_offset;
    let mask = if bit_len == 8 {
        u8::MAX
    } else {
        (1u8 << bit_len) - 1
    };

    shifted & mask
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    #[cfg(target_arch = "x86_64")]
    use super::count_ones_avx2;
    #[cfg(target_arch = "x86_64")]
    use super::count_ones_avx512;
    #[cfg(target_arch = "x86_64")]
    use super::count_ones_popcnt;
    #[cfg(target_arch = "x86_64")]
    use super::count_ones_portable;
    use crate::BitBuffer;

    #[cfg_attr(miri, ignore)]
    #[rstest]
    fn test_count_ones_matches_iteration_for_slices(
        #[values(
            0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
            23, 24, 25, 26, 27, 28, 29, 30
        )]
        offset: usize,
        #[values(
            0usize, 1, 2, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 255, 256, 257, 513
        )]
        slice_len: usize,
    ) {
        let len = 513;
        let buf = BitBuffer::collect_bool(len + 31, |i| (i % 3 == 0) ^ (i % 11 == 0));

        if offset + slice_len > buf.len() {
            return;
        }

        let sliced = buf.slice(offset..offset + slice_len);
        let expected = sliced.iter().filter(|bit| *bit).count();

        assert_eq!(
            sliced.true_count(),
            expected,
            "offset={offset} len={slice_len}"
        );
    }

    /// Lengths spanning many 64-byte SIMD chunks plus a scalar tail.
    #[cfg_attr(miri, ignore)]
    #[rstest]
    fn test_count_ones_matches_iteration_multi_chunk(
        #[values(0usize, 5)] offset: usize,
        #[values(1024usize, 4096, 4097, 65_537)] len: usize,
    ) {
        let buf = BitBuffer::collect_bool(offset + len, |i| (i % 3 == 0) ^ (i % 7 == 0));
        let sliced = buf.slice(offset..offset + len);
        let expected = sliced.iter().filter(|bit| *bit).count();

        assert_eq!(sliced.true_count(), expected, "offset={offset} len={len}");
    }

    /// Every kernel the host supports must match the portable count, not only the one the
    /// dispatcher selects.
    #[cfg(target_arch = "x86_64")]
    #[cfg_attr(miri, ignore)]
    #[rstest]
    fn test_count_ones_x86_kernels_match_portable(
        #[values(0usize, 3, 8)] offset: usize,
        #[values(0usize, 7, 64, 255, 256, 511, 4097, 65_537)] len: usize,
    ) {
        let buf = BitBuffer::collect_bool(offset + len, |i| (i % 3 == 0) ^ (i % 7 == 0));
        let bytes = buf.inner().as_slice();
        let expected = count_ones_portable(bytes, offset, len);

        if is_x86_feature_detected!("popcnt") {
            // SAFETY: `popcnt` was detected above.
            assert_eq!(unsafe { count_ones_popcnt(bytes, offset, len) }, expected);
        }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("popcnt") {
            // SAFETY: `avx2` and `popcnt` were detected above.
            assert_eq!(unsafe { count_ones_avx2(bytes, offset, len) }, expected);
        }
        if is_x86_feature_detected!("avx512f") && is_x86_feature_detected!("avx512vpopcntdq") {
            // SAFETY: `avx512f` and `avx512vpopcntdq` were detected above, and both imply `popcnt`.
            assert_eq!(unsafe { count_ones_avx512(bytes, offset, len) }, expected);
        }
    }
}
