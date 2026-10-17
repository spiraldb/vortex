// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Shared lane kernels, with Vortex storage conformance tests.

pub use rowfn_kernels::lane_kernels::map_into::*;

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use std::mem::MaybeUninit;

    use vortex_buffer::BitBuffer;
    use vortex_buffer::BitBufferMut;

    use super::*;

    fn write_t<T: Copy>(out: Vec<MaybeUninit<T>>) -> Vec<T> {
        // SAFETY: tests always fully initialize the buffer.
        unsafe { std::mem::transmute(out) }
    }

    #[test]
    fn try_map_masked_into_all_ok() {
        let values: Vec<u64> = (0..200).collect();
        let mask = BitBuffer::new_set(200);
        let mut out = vec![MaybeUninit::<u32>::uninit(); 200];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert!(res.is_ok());
        let got = write_t(out);
        assert_eq!(got, (0..200u32).collect::<Vec<_>>());
    }

    #[test]
    fn try_map_masked_into_overflow_fails() {
        let mut values: Vec<u64> = (0..200).collect();
        values[137] = (u32::MAX as u64) + 1;
        let mask = BitBuffer::new_set(200);
        let mut out = vec![MaybeUninit::<u32>::uninit(); 200];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert_eq!(res, Err(137));
    }

    #[test]
    fn try_map_masked_into_overflow_reports_first_failing_lane() {
        let mut values: Vec<u64> = (0..200).collect();
        values[50] = u64::MAX;
        values[51] = u64::MAX;
        values[137] = u64::MAX;
        let mask = BitBuffer::new_set(200);
        let mut out = vec![MaybeUninit::<u32>::uninit(); 200];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert_eq!(res, Err(50));
    }

    #[test]
    fn try_map_masked_into_value_only_closure_filters_null_overflow() {
        let mut values: Vec<u64> = (0..200).collect();
        values[5] = u64::MAX;
        values[42] = u64::MAX;
        let mask = {
            let mut m = BitBufferMut::with_capacity(200);
            for i in 0..200 {
                m.append(i != 5 && i != 42);
            }
            m.freeze()
        };
        let mut out = vec![MaybeUninit::<u32>::uninit(); 200];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert!(
            res.is_ok(),
            "null-lane overflow should be filtered by the cold path"
        );
    }

    #[test]
    fn try_map_masked_into_value_only_closure_reports_first_valid_failure() {
        let mut values: Vec<u64> = (0..200).collect();
        values[5] = u64::MAX;
        values[42] = u64::MAX;
        values[77] = u64::MAX;
        values[100] = u64::MAX;
        let mask = {
            let mut m = BitBufferMut::with_capacity(200);
            for i in 0..200 {
                m.append(i != 5 && i != 42);
            }
            m.freeze()
        };
        let mut out = vec![MaybeUninit::<u32>::uninit(); 200];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert_eq!(res, Err(77));
    }

    #[test]
    fn try_map_masked_into_null_lane_bypasses_check() {
        let mut values: Vec<u64> = (0..200).collect();
        values[5] = u64::MAX;
        let mask = {
            let mut m = BitBufferMut::with_capacity(200);
            for i in 0..200 {
                m.append(i != 5);
            }
            m.freeze()
        };
        let mut out = vec![MaybeUninit::<u32>::uninit(); 200];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert!(res.is_ok());
        let got = write_t(out);
        assert_eq!(got[5], 0);
        assert_eq!(got[6], 6);
    }

    #[test]
    fn try_map_masked_into_branchful_matches_branchless() {
        let mut values: Vec<u64> = (0..130).map(|i| i as u64 * 7).collect();
        values[2] = u64::MAX;
        values[65] = u32::MAX as u64;
        let mask = {
            let mut m = BitBufferMut::with_capacity(130);
            for i in 0..130 {
                m.append(!matches!(i, 2 | 17 | 99));
            }
            m.freeze()
        };

        let mut branchless = vec![MaybeUninit::<u32>::uninit(); 130];
        let mut branchful = vec![MaybeUninit::<u32>::uninit(); 130];
        values
            .as_slice()
            .try_map_masked_into(&mask, &mut branchless, |v| {
                (v <= u32::MAX as u64).then_some(v as u32)
            })
            .unwrap();
        values
            .as_slice()
            .try_map_masked_into(&mask, &mut branchful, |v| u32::try_from(v).ok())
            .unwrap();

        assert_eq!(write_t(branchful), write_t(branchless));
    }

    #[test]
    fn try_map_masked_into_partial_chunk() {
        let values: Vec<u64> = (0..130).collect();
        let mask = BitBuffer::new_set(130);
        let mut out = vec![MaybeUninit::<u32>::uninit(); 130];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert!(res.is_ok());
        let got = write_t(out);
        assert_eq!(got.len(), 130);
        assert_eq!(got[129], 129);
    }

    #[test]
    fn try_map_masked_into_sliced_mask_unaligned_offset() {
        let big = BitBuffer::new_set(256);
        let mask = big.slice(13..143);
        assert_eq!(mask.len(), 130);

        let values: Vec<u64> = (0..130).collect();
        let mut out = vec![MaybeUninit::<u32>::uninit(); 130];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert!(res.is_ok());
        let got = write_t(out);
        assert_eq!(got, (0..130u32).collect::<Vec<_>>());
    }

    #[test]
    fn try_map_masked_into_sliced_mask_with_overflow() {
        let big = BitBuffer::new_set(256);
        let mask = big.slice(13..143);
        assert_eq!(mask.len(), 130);

        let mut values: Vec<u64> = (0..130).collect();
        values[77] = u64::MAX;
        let mut out = vec![MaybeUninit::<u32>::uninit(); 130];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert_eq!(res, Err(77));
    }

    #[test]
    fn try_map_masked_into_sliced_mask_null_lanes() {
        let mut m = BitBufferMut::with_capacity(256);
        for i in 0..256 {
            m.append(i % 3 != 0);
        }
        let big = m.freeze();
        let mask = big.slice(13..143);
        assert_eq!(mask.len(), 130);

        let mut values: Vec<u64> = (0..130).collect();
        values[2] = u64::MAX;
        let mut out = vec![MaybeUninit::<u32>::uninit(); 130];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert!(res.is_ok(), "null lane should bypass the range check");
    }

    #[test]
    fn map_checked_into_writes_all_lanes_and_reduces_flag() {
        let mut values: Vec<u64> = (0..130).collect();
        let mut out = vec![MaybeUninit::<u32>::uninit(); 130];
        let failed = values
            .as_slice()
            .map_checked_into(&mut out, |v| (v as u32, v > u32::MAX as u64));
        assert!(!failed);
        assert_eq!(write_t(out), (0..130u32).collect::<Vec<_>>());

        values[77] = (u32::MAX as u64) + 1;
        let mut out = vec![MaybeUninit::<u32>::uninit(); 130];
        let failed = values
            .as_slice()
            .map_checked_into(&mut out, |v| (v as u32, v > u32::MAX as u64));
        assert!(failed);
        // Failing lanes still write their (wrapped) value.
        assert_eq!(write_t(out)[76], 76);
    }

    #[test]
    fn map_bits_into_packs_full_and_remainder_words() {
        let values: Vec<u32> = (0..130).collect();
        let mut words = vec![u64::MAX; 3];
        values.as_slice().map_bits_into(&mut words, |v| v % 2 == 0);

        for idx in 0..130 {
            let bit = (words[idx / 64] >> (idx % 64)) & 1;
            assert_eq!(bit == 1, idx % 2 == 0, "lane {idx}");
        }
        // Bits past `len` in the remainder word must be written as zero.
        assert_eq!(words[2] >> 2, 0);
    }

    #[test]
    fn map_bits_into_lane_zip_compare() {
        use crate::lane_kernels::LaneZip;

        let lhs: Vec<i64> = (0..100).collect();
        let rhs: Vec<i64> = (0..100).rev().collect();
        let mut words = vec![0u64; 2];
        LaneZip::new(lhs.as_slice(), rhs.as_slice()).map_bits_into(&mut words, |(a, b)| a >= b);

        for idx in 0..100 {
            let bit = (words[idx / 64] >> (idx % 64)) & 1;
            assert_eq!(bit == 1, lhs[idx] >= rhs[idx], "lane {idx}");
        }
    }

    #[test]
    #[should_panic(expected = "words slice has 1 entries")]
    fn map_bits_into_words_too_short_panics() {
        let values: Vec<u32> = (0..65).collect();
        let mut words = vec![0u64; 1];
        values.as_slice().map_bits_into(&mut words, |v| v > 0);
    }

    #[test]
    fn try_map_masked_into_overflow_in_remainder() {
        let mut values: Vec<u64> = (0..130).collect();
        values[129] = (u32::MAX as u64) + 1;
        let mask = BitBuffer::new_set(130);
        let mut out = vec![MaybeUninit::<u32>::uninit(); 130];
        let res = values.as_slice().try_map_masked_into(&mask, &mut out, |v| {
            (v <= u32::MAX as u64).then_some(v as u32)
        });
        assert_eq!(res, Err(129));
    }
}
