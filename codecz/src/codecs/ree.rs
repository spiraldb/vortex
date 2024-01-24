use super::{Codec, CodecError, CodecFunction, ALIGNED_ALLOCATOR};
use codecz_sys::alloc::AlignedVec;
use codecz_sys::*;
use safe_transmute::TriviallyTransmutable;

pub fn encode<T: SupportsREE>(elems: &[T]) -> Result<(AlignedVec<T>, AlignedVec<u32>), CodecError> {
    // TODO: can use smaller buffers if we have stats wired through
    let mut values: AlignedVec<T> = AlignedVec::with_capacity_in(elems.len(), ALIGNED_ALLOCATOR);
    let mut run_ends: AlignedVec<u32> =
        AlignedVec::with_capacity_in(elems.len(), ALIGNED_ALLOCATOR);

    let encoded: TwoBufferResult_t =
        T::encode_impl(elems, (&mut values).into(), (&mut run_ends).into());

    unsafe {
        values.set_len(encoded.firstBuffer.numElements.try_into().unwrap());
        run_ends.set_len(encoded.secondBuffer.numElements.try_into().unwrap());
    }

    if let Some(e) = CodecError::parse_error(encoded.status, Codec::REE, CodecFunction::Encode) {
        return Err(e);
    }

    values.shrink_to_fit();
    run_ends.shrink_to_fit();

    Ok((values, run_ends))
}

pub fn decode<T: SupportsREE>(values: &[T], run_ends: &[u32]) -> Result<AlignedVec<T>, CodecError> {
    let mut decoded: AlignedVec<T> = AlignedVec::with_capacity_in(
        run_ends.last().map(|x| *x as usize).unwrap_or(0_usize),
        ALIGNED_ALLOCATOR,
    );

    let result: OneBufferResult_t = T::decode_impl(
        values.into(),
        run_ends.into(),
        run_ends.len(),
        (&mut decoded).into(),
    );

    unsafe {
        decoded.set_len(result.buffer.numElements.try_into().unwrap());
    }

    if let Some(e) = CodecError::parse_error(result.status, Codec::REE, CodecFunction::Decode) {
        return Err(e);
    }

    decoded.shrink_to_fit();

    Ok(decoded)
}

pub trait SupportsREE: Sized + TriviallyTransmutable {
    fn encode_impl(
        elems: &[Self],
        values_buf: ByteBuffer_t,
        runends_buf: ByteBuffer_t,
    ) -> TwoBufferResult_t;

    fn decode_impl(
        values: ByteBuffer_t,
        runends: ByteBuffer_t,
        num_runs: usize,
        out: ByteBuffer_t,
    ) -> OneBufferResult_t;
}

macro_rules! impl_ree {
    ($t:ty) => {
        paste::item! {
            impl SupportsREE for $t {
                fn encode_impl(
                    elems: &[Self],
                    values_buf: ByteBuffer_t,
                    runends_buf: ByteBuffer_t,
                ) -> TwoBufferResult_t {
                    unsafe { [<codecz_ree_encode_ $t _u32>](elems.as_ptr(), elems.len(), values_buf, runends_buf) }
                }

                fn decode_impl(
                    values: ByteBuffer_t,
                    runends: ByteBuffer_t,
                    num_runs: usize,
                    out: ByteBuffer_t,
                ) -> OneBufferResult_t {
                    unsafe { [<codecz_ree_decode_ $t _u32>](values, runends, num_runs, out) }
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ree() {
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
