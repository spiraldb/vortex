use super::{
    AlignedVec, Codec, CodecError, CodecFunction, OneBufferResult, WrittenBuffer, ALIGNED_ALLOCATOR,
};
use codecz_sys::{
    codecz_zz_decode_i16, codecz_zz_decode_i32, codecz_zz_decode_i64, codecz_zz_decode_i8,
    codecz_zz_encode_i16, codecz_zz_encode_i32, codecz_zz_encode_i64, codecz_zz_encode_i8,
};
use num_traits::PrimInt;
use safe_transmute::TriviallyTransmutable;

pub fn encode<T: SupportsZigZag>(elems: &[T]) -> Result<AlignedVec<T::Unsigned>, CodecError> {
    let mut unsigned: AlignedVec<T::Unsigned> =
        AlignedVec::with_capacity_in(elems.len(), ALIGNED_ALLOCATOR);

    let unsigned_buf = T::encode_impl(elems, &mut unsigned)?;
    assert_eq!(
        unsigned_buf.numElements,
        elems.len() as u64,
        "ZigZag: encoded buffer has length {} but should have length {}",
        unsigned_buf.numElements,
        elems.len()
    );
    unsafe {
        unsigned.set_len(unsigned_buf.numElements as usize);
    }
    unsigned.shrink_to_fit();

    Ok(unsigned)
}

pub fn decode<T: SupportsZigZag>(unsigned: &[T::Unsigned]) -> Result<AlignedVec<T>, CodecError> {
    let mut decoded: AlignedVec<T> =
        AlignedVec::with_capacity_in(unsigned.len(), ALIGNED_ALLOCATOR);

    let decoded_buf = T::decode_impl(unsigned, &mut decoded)?;
    assert_eq!(
        decoded_buf.numElements,
        unsigned.len() as u64,
        "ZigZag: decoded buffer has length {} but should have length {}",
        decoded_buf.numElements,
        unsigned.len()
    );
    unsafe {
        decoded.set_len(unsigned.len());
    }
    decoded.shrink_to_fit();

    Ok(decoded)
}

pub trait SupportsZigZag: Sized + TriviallyTransmutable + PrimInt {
    type Unsigned: Sized + TriviallyTransmutable + PrimInt;

    fn encode_impl(
        elems: &[Self],
        out: &mut AlignedVec<Self::Unsigned>,
    ) -> Result<WrittenBuffer, CodecError>;

    fn decode_impl(
        encoded: &[Self::Unsigned],
        out: &mut AlignedVec<Self>,
    ) -> Result<WrittenBuffer, CodecError>;
}

macro_rules! impl_zz {
    ($t:ty, $u:ty) => {
        paste::item! {
            impl SupportsZigZag for $t {
                type Unsigned = $u;

                fn encode_impl(elems: &[Self], out: &mut AlignedVec<Self::Unsigned>) -> Result<WrittenBuffer, CodecError>{
                    let mut result = OneBufferResult::new(out);
                    unsafe {
                        [<codecz_zz_encode_ $t>](
                            elems.as_ptr(),
                            elems.len() as u64,
                            &mut result as *mut OneBufferResult
                        );
                    }
                    if let Some(e) = CodecError::parse_error(result.status, Codec::ZigZag, CodecFunction::Encode) {
                        return Err(e);
                    }
                    Ok(result.buf)
                }

                fn decode_impl(
                    encoded: &[Self::Unsigned],
                    out: &mut AlignedVec<Self>,
                ) -> Result<WrittenBuffer, CodecError> {
                    let mut result = OneBufferResult::new(out);
                    unsafe {
                        [<codecz_zz_decode_ $t>](
                            encoded.as_ptr(),
                            encoded.len() as u64,
                            &mut result as *mut OneBufferResult
                        );
                    }
                    if let Some(e) = CodecError::parse_error(result.status, Codec::ZigZag, CodecFunction::Decode) {
                        return Err(e);
                    }
                    Ok(result.buf)
                }
            }
        }
    };
}

impl_zz!(i8, u8);
impl_zz!(i16, u16);
impl_zz!(i32, u32);
impl_zz!(i64, u64);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_round_trip() {
        let vec = vec![0, -1, 1, -2, 2, -3, 3, -4, 4, -5];
        let values = encode(&vec).unwrap();
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(values.as_ptr()));
        assert_eq!(
            values.as_slice(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].as_slice()
        );

        let decoded = decode::<i32>(&values).unwrap();
        assert_eq!(decoded.as_slice(), vec.as_slice());
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(decoded.as_ptr()));
    }
}
