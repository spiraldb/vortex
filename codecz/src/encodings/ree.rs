use super::{
    AlignedVec, ByteBuffer, Codec, CodecError, CodecFunction, WrittenBuffer, ALIGNED_ALLOCATOR,
};
use codecz_sys::{
    codecz_ree_decode_f16_u32, codecz_ree_decode_f32_u32, codecz_ree_decode_f64_u32,
    codecz_ree_decode_i16_u32, codecz_ree_decode_i32_u32, codecz_ree_decode_i64_u32,
    codecz_ree_decode_i8_u32, codecz_ree_decode_u16_u32, codecz_ree_decode_u32_u32,
    codecz_ree_decode_u64_u32, codecz_ree_decode_u8_u32, codecz_ree_encode_f16_u32,
    codecz_ree_encode_f32_u32, codecz_ree_encode_f64_u32, codecz_ree_encode_i16_u32,
    codecz_ree_encode_i32_u32, codecz_ree_encode_i64_u32, codecz_ree_encode_i8_u32,
    codecz_ree_encode_u16_u32, codecz_ree_encode_u32_u32, codecz_ree_encode_u64_u32,
    codecz_ree_encode_u8_u32,
};
use half::f16;

pub fn encode<T: SupportsREE>(elems: &[T]) -> Result<(AlignedVec<T>, AlignedVec<u32>), CodecError> {
    // TODO: can use smaller buffers if we have stats wired through
    let mut values: AlignedVec<T> = AlignedVec::with_capacity_in(elems.len(), ALIGNED_ALLOCATOR);
    let mut run_ends: AlignedVec<u32> =
        AlignedVec::with_capacity_in(elems.len(), ALIGNED_ALLOCATOR);

    let (values_buf, run_ends_buf) =
        T::encode_impl(elems, (&mut values).into(), (&mut run_ends).into())?;
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
    let capacity = run_ends.last().map(|x| *x as usize).unwrap_or(0_usize);
    let mut decoded: AlignedVec<T> = AlignedVec::with_capacity_in(capacity, ALIGNED_ALLOCATOR);

    let decoded_buf = T::decode_impl(
        values.into(),
        run_ends.into(),
        run_ends.len(),
        (&mut decoded).into(),
    )?;
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
        values_buf: ByteBuffer,
        runends_buf: ByteBuffer,
    ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError>;

    fn decode_impl(
        values: ByteBuffer,
        runends: ByteBuffer,
        num_runs: usize,
        out: ByteBuffer,
    ) -> Result<WrittenBuffer, CodecError>;
}

macro_rules! impl_ree {
    ($t:ty) => {
        paste::item! {
            impl SupportsREE for $t {
                fn encode_impl(
                    elems: &[Self],
                    values_buf: ByteBuffer,
                    runends_buf: ByteBuffer,
                ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError> {
                    let result = unsafe { [<codecz_ree_encode_ $t _u32>](elems.as_ptr(), elems.len() as u64, values_buf, runends_buf) };
                    if let Some(e) = CodecError::parse_error(result.status, Codec::REE, CodecFunction::Encode) {
                        return Err(e);
                    }
                    Ok((result.firstBuffer, result.secondBuffer))
                }

                fn decode_impl(
                    values: ByteBuffer,
                    runends: ByteBuffer,
                    num_runs: usize,
                    out: ByteBuffer,
                ) -> Result<WrittenBuffer, CodecError>{
                    let result = unsafe { [<codecz_ree_decode_ $t _u32>](values, runends, num_runs as u64, out) };
                    if let Some(e) = CodecError::parse_error(result.status, Codec::REE, CodecFunction::Decode) {
                        return Err(e);
                    }
                    Ok(result.buffer)
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
        values_buf: ByteBuffer,
        runends_buf: ByteBuffer,
    ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError> {
        let result = unsafe {
            codecz_ree_encode_f16_u32(
                elems.as_ptr() as *const i16,
                elems.len() as u64,
                values_buf,
                runends_buf,
            )
        };
        if let Some(e) = CodecError::parse_error(result.status, Codec::REE, CodecFunction::Encode) {
            return Err(e);
        }
        Ok((result.firstBuffer, result.secondBuffer))
    }

    fn decode_impl(
        values: ByteBuffer,
        runends: ByteBuffer,
        num_runs: usize,
        out: ByteBuffer,
    ) -> Result<WrittenBuffer, CodecError> {
        let result = unsafe { codecz_ree_decode_f16_u32(values, runends, num_runs as u64, out) };
        if let Some(e) = CodecError::parse_error(result.status, Codec::REE, CodecFunction::Decode) {
            return Err(e);
        }
        Ok(result.buffer)
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
