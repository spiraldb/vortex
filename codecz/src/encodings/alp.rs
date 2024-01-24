use super::{
    AlignedVec, ByteBuffer, Codec, CodecError, CodecFunction, WrittenBuffer, ALIGNED_ALLOCATOR,
};
use codecz_sys::{
    codecz_alp_decode_f32, codecz_alp_decode_f64, codecz_alp_encode_f32, codecz_alp_encode_f64,
    codecz_alp_sampleFindExponents_f32, codecz_alp_sampleFindExponents_f64,
};
use safe_transmute::TriviallyTransmutable;

pub type ALPExponents = codecz_sys::AlpExponents_t;
pub struct ALPEncoded<EncInt> {
    pub values: AlignedVec<EncInt>,
    pub exponents: ALPExponents,
    pub exceptions_idx: AlignedVec<u8>,
}

pub fn encode<T: SupportsALP>(elems: &[T]) -> Result<ALPEncoded<T::EncInt>, CodecError> {
    let exponents = T::find_exponents_impl(elems)?;
    let (values, exceptions_idx) = encode_with(elems, exponents)?;
    Ok(ALPEncoded {
        values,
        exponents,
        exceptions_idx,
    })
}

pub fn encode_with<T: SupportsALP>(
    elems: &[T],
    exponents: ALPExponents,
) -> Result<(AlignedVec<T::EncInt>, AlignedVec<u8>), CodecError> {
    let mut values: AlignedVec<T::EncInt> =
        AlignedVec::with_capacity_in(elems.len(), ALIGNED_ALLOCATOR);

    let bitset_size_in_bytes = elems.len().div_ceil(8);
    let mut exceptions_idx: AlignedVec<u8> =
        AlignedVec::with_capacity_in(bitset_size_in_bytes, ALIGNED_ALLOCATOR);

    let (values_buf, exceptions_idx_buf) = T::encode_impl(
        elems,
        exponents,
        (&mut values).into(),
        (&mut exceptions_idx).into(),
    )?;

    assert_eq!(
        values_buf.numElements,
        elems.len() as u64,
        "ALP: values buffer has length {} but should have length {}",
        values_buf.numElements,
        elems.len()
    );
    assert_eq!(
        // for the bitset, numElements is the cardinality of the bitset
        exceptions_idx_buf.inputBytesUsed,
        bitset_size_in_bytes as u64,
        "ALP: exceptions_idx buffer has length {} but should have length {}",
        exceptions_idx_buf.inputBytesUsed,
        bitset_size_in_bytes
    );
    unsafe {
        values.set_len(elems.len());
        exceptions_idx.set_len(bitset_size_in_bytes);
    }

    // TODO: find a better way of returning bitset. Right now we don't utilize the fact that
    // exceptions_idx_buf.numElements is the cardinality of the bitset
    Ok((values, exceptions_idx))
}

pub fn decode<T: SupportsALP>(
    values: &[T::EncInt],
    exponents: ALPExponents,
) -> Result<AlignedVec<T>, CodecError> {
    let mut decoded: AlignedVec<T> = AlignedVec::with_capacity_in(values.len(), ALIGNED_ALLOCATOR);

    let decoded_buf = T::decode_impl(values, exponents, (&mut decoded).into())?;
    assert_eq!(
        decoded_buf.numElements,
        values.len() as u64,
        "ALP: decoded buffer has length {} but should have length {}",
        decoded_buf.numElements,
        values.len()
    );
    unsafe {
        decoded.set_len(values.len());
    }

    Ok(decoded)
}

pub trait SupportsALP: Sized + TriviallyTransmutable {
    type EncInt: TriviallyTransmutable;

    fn find_exponents_impl(elems: &[Self]) -> Result<ALPExponents, CodecError>;

    fn encode_impl(
        elems: &[Self],
        exponents: ALPExponents,
        values_buf: ByteBuffer,
        exceptions_idx_buf: ByteBuffer,
    ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError>;

    fn decode_impl(
        encoded: &[Self::EncInt],
        exponents: ALPExponents,
        out: ByteBuffer,
    ) -> Result<WrittenBuffer, CodecError>;
}

macro_rules! impl_alp {
    ($t:ty, $e:ty) => {
        paste::item! {
            impl SupportsALP for $t {
                type EncInt = $e;

                fn find_exponents_impl(elems: &[Self]) -> Result<ALPExponents, CodecError> {
                    let result = unsafe { [<codecz_alp_sampleFindExponents_ $t>](elems.as_ptr(), elems.len() as u64) };
                    if let Some(e) = CodecError::parse_error(result.status, Codec::ALP, CodecFunction::Prelude) {
                        return Err(e);
                    }
                    Ok(result.exponents)
                }

                fn encode_impl(
                    elems: &[Self],
                    exponents: ALPExponents,
                    values_buf: ByteBuffer,
                    exceptions_idx_buf: ByteBuffer,
                ) -> Result<(WrittenBuffer, WrittenBuffer), CodecError> {
                    let result = unsafe { [<codecz_alp_encode_ $t>](elems.as_ptr(), elems.len() as u64, exponents, values_buf, exceptions_idx_buf) };
                    if let Some(e) = CodecError::parse_error(result.status, Codec::ALP, CodecFunction::Encode) {
                        return Err(e);
                    }
                    Ok((result.firstBuffer, result.secondBuffer))
                }

                fn decode_impl(
                    encoded: &[Self::EncInt],
                    exponents: ALPExponents,
                    out: ByteBuffer,
                ) -> Result<WrittenBuffer, CodecError> {
                    let result = unsafe { [<codecz_alp_decode_ $t>](encoded.as_ptr(), encoded.len() as u64, exponents, out) };
                    if let Some(e) = CodecError::parse_error(result.status, Codec::ALP, CodecFunction::Decode) {
                        return Err(e);
                    }
                    Ok(result.buffer)
                }
            }
        }
    };
}

impl_alp!(f32, i32);
impl_alp!(f64, i64);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_round_trip() {
        let vec = vec![1.0, 1.1, 2.73, 4.567, 42.4247];
        let encoded = encode(&vec).unwrap();

        assert!(ALIGNED_ALLOCATOR.is_aligned_to(encoded.values.as_ptr()));
        assert_eq!(encoded.exponents.e - encoded.exponents.f, 4);
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(encoded.exceptions_idx.as_ptr()));
        assert_eq!(
            encoded.values.as_slice(),
            vec![10000i64, 11000, 27300, 45670, 424247].as_slice()
        );
        assert_eq!(encoded.exceptions_idx.as_slice(), vec![0u8].as_slice());

        let decoded = decode::<f64>(&encoded.values, encoded.exponents).unwrap();
        assert_eq!(decoded.as_slice(), vec.as_slice());
        assert!(ALIGNED_ALLOCATOR.is_aligned_to(decoded.as_ptr()));
    }
}
