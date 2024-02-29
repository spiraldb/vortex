// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{
    AlignedVec, Codec, CodecError, CodecFunction, OneBufferResult, TwoBufferResult, WrittenBuffer,
    ALIGNED_ALLOCATOR,
};
use codecz_sys::*;
use half::f16;

pub fn encode<T: SupportsREE>(elems: &[T]) -> Result<(AlignedVec<T>, AlignedVec<u32>), CodecError> {
    // TODO: can use smaller buffers if we have stats wired through
    let mut values: AlignedVec<T> = AlignedVec::with_capacity_in(elems.len(), ALIGNED_ALLOCATOR);
    let mut run_ends: AlignedVec<u32> =
        AlignedVec::with_capacity_in(elems.len(), ALIGNED_ALLOCATOR);

    let (values_buf, run_ends_buf) = T::encode_impl(elems, &mut values, &mut run_ends)?;
    assert_eq!(
        values_buf.numElements, run_ends_buf.numElements,
        "REE: values and run ends have different lengths of {} and {}, respectively",
        values_buf.numElements, run_ends_buf.numElements
    );

    unsafe {
        values.set_len(values_buf.numElements as usize);
        run_ends.set_len(run_ends_buf.numElements as usize);
    }
    values.shrink_to_fit();
    run_ends.shrink_to_fit();

    Ok((values, run_ends))
}

pub fn decode<T: SupportsREE>(values: &[T], run_ends: &[u32]) -> Result<AlignedVec<T>, CodecError> {
    if values.len() != run_ends.len() {
        return Err(CodecError::InvalidInput(Codec::REE, CodecFunction::Decode));
    }

    let capacity = run_ends.last().map(|x| *x as usize).unwrap_or(0_usize);
    let mut decoded: AlignedVec<T> = AlignedVec::with_capacity_in(capacity, ALIGNED_ALLOCATOR);

    let decoded_buf = T::decode_impl(values, run_ends, &mut decoded)?;
    unsafe {
        decoded.set_len(capacity);
    }
    assert_eq!(
        decoded_buf.numElements, capacity as u64,
        "REE: decoded buffer has length {} but should have length {}",
        decoded_buf.numElements, capacity
    );

    Ok(decoded)
}

pub trait SupportsREE: Sized {
    fn encode_impl(
        elems: &[Self],
        values: &mut AlignedVec<Self>,
        run_ends: &mut AlignedVec<u32>,
    ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError>;

    fn decode_impl(
        values: &[Self],
        run_ends: &[u32],
        decoded: &mut AlignedVec<Self>,
    ) -> Result<WrittenBuffer, CodecError>;
}

macro_rules! impl_ree {
    ($t:ty) => {
        paste::item! {
            impl SupportsREE for $t {
                fn encode_impl(
                    elems: &[Self],
                    values: &mut AlignedVec<Self>,
                    run_ends: &mut AlignedVec<u32>,
                ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError> {
                    let mut result = TwoBufferResult::new(values, run_ends);
                    unsafe { [<codecz_ree_encode_ $t _u32>](elems.as_ptr(), elems.len() as u64, &mut result as *mut TwoBufferResult); };
                    if let Some(e) = CodecError::parse_error(result.status, Codec::REE, CodecFunction::Encode) {
                        return Err(e);
                    }
                    Ok((result.first, result.second))
                }

                fn decode_impl(
                    values: &[Self],
                    run_ends: &[u32],
                    decoded: &mut AlignedVec<Self>,
                ) -> Result<WrittenBuffer, CodecError>{
                    let mut result = OneBufferResult::new(decoded);
                    assert_eq!(values.len(), run_ends.len());
                    unsafe {
                        [<codecz_ree_decode_ $t _u32>](
                            values.as_ptr(),
                            run_ends.as_ptr(),
                            values.len() as u64,
                            &mut result as *mut OneBufferResult
                        );
                    };
                    if let Some(e) = CodecError::parse_error(result.status, Codec::REE, CodecFunction::Decode) {
                        return Err(e);
                    }
                    Ok(result.buf)
                }
            }
        }
    };
}

impl_ree!(u8);
impl_ree!(u16);
impl_ree!(u32);
impl_ree!(u64);
impl_ree!(i8);
impl_ree!(i16);
impl_ree!(i32);
impl_ree!(i64);
impl_ree!(f32);
impl_ree!(f64);

impl SupportsREE for f16 {
    fn encode_impl(
        elems: &[Self],
        values: &mut AlignedVec<Self>,
        run_ends: &mut AlignedVec<u32>,
    ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError> {
        let mut result = TwoBufferResult::new(values, run_ends);
        unsafe {
            codecz_ree_encode_f16_u32(
                elems.as_ptr() as *const i16,
                elems.len() as u64,
                &mut result as *mut TwoBufferResult,
            );
        };

        if let Some(e) = CodecError::parse_error(result.status, Codec::REE, CodecFunction::Encode) {
            return Err(e);
        }
        Ok((result.first, result.second))
    }

    fn decode_impl(
        values: &[Self],
        run_ends: &[u32],
        decoded: &mut AlignedVec<Self>,
    ) -> Result<WrittenBuffer, CodecError> {
        let mut result = OneBufferResult::new(decoded);
        unsafe {
            codecz_ree_decode_f16_u32(
                values.as_ptr() as *const i16,
                run_ends.as_ptr(),
                values.len() as u64,
                &mut result as *mut OneBufferResult,
            )
        };
        if let Some(e) = CodecError::parse_error(result.status, Codec::REE, CodecFunction::Decode) {
            return Err(e);
        }
        Ok(result.buf)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_round_trip() {
        let vec = vec![1, 1, 1, 2, 3, 4, 4, 5];
        let (values, run_ends) = encode(&vec).unwrap();
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(values.as_ptr()));
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(run_ends.as_ptr()));
        assert_eq!(values.as_slice(), vec![1, 2, 3, 4, 5].as_slice());
        assert_eq!(run_ends.as_slice(), vec![3, 4, 5, 7, 8].as_slice());

        let decoded = decode(&values, &run_ends).unwrap();
        assert_eq!(decoded.as_slice(), vec.as_slice());
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(decoded.as_ptr()));
    }
}
