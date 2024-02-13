use std::mem::size_of;

use arrow_buffer::BooleanBuffer;
use num_traits::PrimInt;
use safe_transmute::TriviallyTransmutable;

use codecz_sys::*;

use super::{
    AlignedVec, ByteBuffer, Codec, CodecError, CodecFunction, OneBufferResult, TwoBufferResult,
    WrittenBuffer, ALIGNED_ALLOCATOR,
};

type FforResult = codecz_sys::OneBufferNumExceptionsResult_t;

pub struct FforEncoded {
    pub buf: AlignedVec<u8>,
    pub num_exceptions: usize,
}

impl FforEncoded {
    pub fn new(buf: AlignedVec<u8>, num_exceptions: usize) -> Self {
        Self {
            buf,
            num_exceptions,
        }
    }
}

pub fn find_best_bit_width<T: SupportsFFoR>(
    bit_width_freq: &[u64],
    min_val: T,
    max_val: T,
) -> Result<u8, CodecError> {
    let total = bit_width_freq.iter().sum::<u64>();
    if total == 0 || min_val > max_val {
        return Err(CodecError::InvalidInput(
            Codec::FFoR,
            CodecFunction::Prelude,
        ));
    }

    let bit_size_of_t = size_of::<T>() * 8;
    // smallest supported type is one byte; the histogram has the bit size of T + 1 elements
    if bit_width_freq.len() != bit_size_of_t + 1 || bit_width_freq.len() > u8::MAX as usize {
        return Err(CodecError::InvalidInput(
            Codec::FFoR,
            CodecFunction::Prelude,
        ));
    }

    // calculate the reverse cumulative sum (i.e., the number of elements *after* this bit width)
    // we effectively shift the histogram
    let max_num_bits = max_bits_for_range(min_val, max_val); // inclusive max
    let num_exceptions_per_bw: Vec<u64> = bit_width_freq
        .iter()
        .scan(0, |acc, x| {
            *acc += *x;
            Some(*acc)
        })
        .map(|x| total - x)
        .collect();

    // pick the bitwidth that minimizes total size
    let best_num_bits = num_exceptions_per_bw
        .iter()
        .enumerate()
        .filter_map(|(bw, num_exceptions)| {
            if bw == 0 || *num_exceptions == total || bw > max_num_bits as usize {
                return None;
            }
            let base_size_bytes = T::encoded_size_in_bytes_impl(total as usize, bw as u8) as u64;
            let exc_size_bytes = num_exceptions.saturating_mul(core::mem::size_of::<T>() as u64);
            Some((bw, base_size_bytes + exc_size_bytes))
        })
        // min by size in bytes
        .min_by(|lhs, rhs| lhs.1.cmp(&rhs.1))
        .map(|(bw, _)| bw as u8);

    // pick the max if no bit width is better
    Ok(best_num_bits.unwrap_or(max_num_bits))
}

fn max_bits_for_range<T: SupportsFFoR>(min_val: T, max_val: T) -> u8 {
    let min_val = min_val.to_i128().unwrap();
    let max_val = max_val.to_i128().unwrap();
    let range_size = min_val.abs_diff(max_val);
    let log2_range_size = range_size.checked_ilog2().unwrap_or(0);
    let range_bits = if 2.pow(log2_range_size) == range_size {
        log2_range_size as u8
    } else {
        log2_range_size as u8 + 1
    };

    range_bits.max(1).min(T::max_packed_bit_width_impl())
}

pub fn encode<T: SupportsFFoR>(
    elems: &[T],
    num_bits: u8,
    min_val: T,
) -> Result<FforEncoded, CodecError> {
    let size_in_bytes = T::encoded_size_in_bytes_impl(elems.len(), num_bits);

    let mut encoded: AlignedVec<u8> =
        AlignedVec::with_capacity_in(size_in_bytes, ALIGNED_ALLOCATOR);

    let (encoded_buf, num_exceptions) = T::encode_impl(elems, num_bits, min_val, &mut encoded)?;
    assert_eq!(
        encoded_buf.inputBytesUsed, size_in_bytes as u64,
        "FFoR: encoded buffer has inputBytesUsed {} but should have inputBytesUsed {}",
        encoded_buf.inputBytesUsed, size_in_bytes
    );
    unsafe {
        encoded.set_len(size_in_bytes);
    }

    Ok(FforEncoded::new(encoded, num_exceptions as usize))
}

pub fn collect_exceptions<T: SupportsFFoR>(
    elems: &[T],
    num_bits: u8,
    min_val: T,
    num_exceptions: usize,
) -> Result<(AlignedVec<T>, BooleanBuffer), CodecError> {
    // there's a quirk of our collect exceptions implementation that it needs one extra element of "scratch" space
    let mut exception_values: AlignedVec<T> =
        AlignedVec::with_capacity_in(num_exceptions + 1, ALIGNED_ALLOCATOR);
    let bitset_size_in_bytes = elems.len().div_ceil(8);
    let mut exception_indices: AlignedVec<u8> =
        AlignedVec::with_capacity_in(bitset_size_in_bytes, ALIGNED_ALLOCATOR);

    let (values_buf, idxs_buf) = T::collect_exceptions_impl(
        elems,
        num_bits,
        min_val,
        num_exceptions,
        &mut exception_values,
        &mut exception_indices,
    )?;
    assert_eq!(
        values_buf.numElements, num_exceptions as u64,
        "FFoR: values buffer has length {} but should have length {}",
        values_buf.numElements, num_exceptions
    );
    assert_eq!(
        values_buf.inputBytesUsed,
        (num_exceptions * std::mem::size_of::<T>()) as u64,
        "FFoR: values buffer has inputBytesUsed {} but should have inputBytesUsed {}",
        values_buf.inputBytesUsed,
        num_exceptions * std::mem::size_of::<T>()
    );
    assert_eq!(
        idxs_buf.numElements, num_exceptions as u64,
        "FFoR: idxs buffer has cardinality {} but should have cardinality {}",
        idxs_buf.numElements, num_exceptions
    );
    assert_eq!(
        idxs_buf.inputBytesUsed, bitset_size_in_bytes as u64,
        "FFoR: idxs buffer has inputBytesUsed {} but should have inputBytesUsed {}",
        idxs_buf.inputBytesUsed, bitset_size_in_bytes
    );
    unsafe {
        exception_values.set_len(num_exceptions);
        exception_indices.set_len(bitset_size_in_bytes);
    }

    let exception_indices = crate::utils::into_boolean_buffer(exception_indices, elems.len());
    Ok((exception_values, exception_indices))
}

pub fn decode<T: SupportsFFoR>(
    encoded: &[u8],
    num_elems: usize,
    num_bits: u8,
    min_val: T,
) -> Result<AlignedVec<T>, CodecError> {
    if T::encoded_size_in_bytes_impl(num_elems, num_bits) != encoded.len() {
        return Err(CodecError::InvalidInput(Codec::FFoR, CodecFunction::Decode));
    }

    let mut decoded: AlignedVec<T> = AlignedVec::with_capacity_in(num_elems, ALIGNED_ALLOCATOR);
    let decoded_buf = T::decode_impl(encoded, num_elems, num_bits, min_val, &mut decoded)?;
    assert_eq!(
        decoded_buf.numElements, num_elems as u64,
        "FFoR: decoded buffer has length {} but should have length {}",
        decoded_buf.numElements, num_elems
    );
    unsafe {
        decoded.set_len(num_elems);
    }

    Ok(decoded)
}

pub trait SupportsFFoR: Sized + TriviallyTransmutable + PrimInt {
    fn encoded_size_in_bytes_impl(len: usize, num_bits: u8) -> usize;

    fn max_packed_bit_width_impl() -> u8;

    fn encode_impl(
        elems: &[Self],
        num_bits: u8,
        min_val: Self,
        out: &mut AlignedVec<u8>,
    ) -> Result<(WrittenBuffer, u64), CodecError>;

    fn collect_exceptions_impl(
        elems: &[Self],
        num_bits: u8,
        min_val: Self,
        num_exceptions: usize,
        exception_values: &mut AlignedVec<Self>,
        exception_indices: &mut AlignedVec<u8>,
    ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError>;

    fn decode_impl(
        encoded: &[u8],
        num_elems: usize,
        num_bits: u8,
        min_val: Self,
        out: &mut AlignedVec<Self>,
    ) -> Result<WrittenBuffer, CodecError>;
}

macro_rules! impl_ffor {
    ($t:ty) => {
        paste::item! {
            impl SupportsFFoR for $t {
                fn encoded_size_in_bytes_impl(len: usize, num_bits: u8) -> usize {
                    unsafe {
                        [<codecz_flbp_encodedSizeInBytes_ $t>](
                            len as u64,
                            num_bits
                        ) as usize
                    }
                }

                fn max_packed_bit_width_impl() -> u8 {
                    unsafe { [<codecz_flbp_maxPackedBitWidth_ $t>]() }
                }

                fn encode_impl(elems: &[Self], num_bits: u8, min_val: Self, out: &mut AlignedVec<u8>) -> Result<(WrittenBuffer, u64), CodecError>{
                    let mut result = FforResult::new(out);
                    unsafe {
                        [<codecz_ffor_encode_ $t>](
                            elems.as_ptr(),
                            elems.len() as u64,
                            num_bits,
                            min_val,
                            &mut result as *mut FforResult
                        );
                    }
                    if let Some(e) = CodecError::parse_error(result.status, Codec::FFoR, CodecFunction::Encode) {
                        return Err(e);
                    }
                    Ok((result.encoded, result.num_exceptions))
                }

                fn collect_exceptions_impl(
                    elems: &[Self],
                    num_bits: u8,
                    min_val: Self,
                    num_exceptions: usize,
                    exception_values: &mut AlignedVec<Self>,
                    exception_indices: &mut AlignedVec<u8>,
                ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError> {
                    let mut result = TwoBufferResult::new(exception_values, exception_indices);
                    unsafe {
                        [<codecz_ffor_collectExceptions_ $t>](
                            elems.as_ptr(),
                            elems.len() as u64,
                            num_bits,
                            min_val,
                            num_exceptions as u64,
                            &mut result as *mut TwoBufferResult
                        );
                    }
                    if let Some(e) = CodecError::parse_error(result.status, Codec::FFoR, CodecFunction::CollectExceptions) {
                        return Err(e);
                    }
                    Ok((result.first, result.second))
                }

                fn decode_impl(
                    encoded: &[u8],
                    num_elems: usize,
                    num_bits: u8,
                    min_val: Self,
                    out: &mut AlignedVec<Self>,
                ) -> Result<WrittenBuffer, CodecError> {
                    let input: ByteBuffer = encoded.into();
                    let mut result = OneBufferResult::new(out);
                    unsafe {
                        [<codecz_ffor_decode_ $t>](
                            &input as *const ByteBuffer,
                            num_elems as u64,
                            num_bits,
                            min_val,
                            &mut result as *mut OneBufferResult
                        );
                    }
                    if let Some(e) = CodecError::parse_error(result.status, Codec::FFoR, CodecFunction::Decode) {
                        return Err(e);
                    }
                    Ok(result.buf)
                }
            }
        }
    };
}

impl_ffor!(i8);
impl_ffor!(i16);
impl_ffor!(i32);
impl_ffor!(i64);
impl_ffor!(u8);
impl_ffor!(u16);
impl_ffor!(u32);
impl_ffor!(u64);

#[cfg(test)]
mod test {
    use crate::min;

    use super::*;

    #[test]
    fn max_packed_bit_width() {
        assert_eq!(i8::max_packed_bit_width_impl(), 7);
        assert_eq!(i16::max_packed_bit_width_impl(), 12);
        assert_eq!(i32::max_packed_bit_width_impl(), 24);
        assert_eq!(i64::max_packed_bit_width_impl(), 48);
        assert_eq!(u8::max_packed_bit_width_impl(), 7);
        assert_eq!(u16::max_packed_bit_width_impl(), 12);
        assert_eq!(u32::max_packed_bit_width_impl(), 24);
        assert_eq!(u64::max_packed_bit_width_impl(), 48);
    }

    #[test]
    fn test_find_bit_width() {
        let bit_width_freqs = vec![
            0u64, 1, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
            0, 0, 0, 0, 0,
        ];
        assert_eq!(
            find_best_bit_width::<u32>(&bit_width_freqs, 1, 33554431).unwrap(),
            3
        );
    }

    #[test]
    fn test_round_trip_no_exceptions() {
        let mut vec: AlignedVec<i32> = AlignedVec::with_capacity_in(10, ALIGNED_ALLOCATOR);
        vec.extend([0i32, -1, 1, -2, 2, -3, 3, -4, 4, -5].iter());
        let vec = vec; // drop mut

        let min_val = min(vec.as_slice());
        let num_bits = 4u8; // 2^4 = 16 values, enough to cover [-5, 10] with no exceptions
        let FforEncoded {
            buf,
            num_exceptions,
        } = encode(&vec, num_bits, min_val).unwrap();

        assert!(ALIGNED_ALLOCATOR.is_aligned_to(buf.as_ptr()));
        assert_eq!(num_exceptions, 0);

        let decoded = decode::<i32>(buf.as_slice(), vec.len(), num_bits, min_val).unwrap();
        assert_eq!(decoded.as_slice(), vec.as_slice());
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(decoded.as_ptr()));
    }

    #[test]
    fn test_round_trip_with_exceptions() {
        let mut vec: AlignedVec<i32> = AlignedVec::with_capacity_in(10, ALIGNED_ALLOCATOR);
        vec.extend([0i32, -1, 1, -2, 2, -3, 3, -4, 4, -5].iter());
        let vec = vec; // drop mut
        let min_val = min(vec.as_slice());
        let num_bits = 3u8; // 2^3 = 8 values, enough to cover [-5, 2] with no exceptions

        let FforEncoded {
            buf,
            num_exceptions,
        } = encode(&vec, num_bits, min_val).unwrap();
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(buf.as_ptr()));
        assert_eq!(num_exceptions, 2);

        let (exceptions, exceptions_idx) =
            collect_exceptions(&vec, num_bits, min_val, num_exceptions).unwrap();
        let exceptions_idx: Vec<usize> = exceptions_idx.set_indices().collect();
        assert_eq!(exceptions.as_slice(), &[3, 4]);
        assert_eq!(exceptions_idx.as_slice(), &[6, 8]);
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(exceptions.as_ptr()));

        let mut decoded = decode::<i32>(buf.as_slice(), vec.len(), num_bits, min_val).unwrap();
        // manually patch
        decoded[6] = 3;
        decoded[8] = 4;
        assert_eq!(decoded.as_slice(), vec.as_slice());
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(decoded.as_ptr()));
    }
}
