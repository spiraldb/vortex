use std::mem::size_of;

use arrayref::array_ref;
use num_traits::{WrappingAdd, WrappingSub};

use fastlanez_sys::{transpose, untranspose, Delta};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::fill::fill_forward;
use vortex::compute::flatten::flatten_primitive;
use vortex::error::VortexResult;
use vortex::match_each_integer_ptype;
use vortex::ptype::NativePType;

use crate::{DeltaArray, DeltaEncoding};

impl EncodingCompression for DeltaEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = array.maybe_primitive()?;

        // Only supports ints
        if !parray.ptype().is_int() {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let parray = array.as_primitive();
        let like_delta = like.map(|l| l.as_any().downcast_ref::<DeltaArray>().unwrap());

        let validity = parray
            .validity()
            .map(|v| {
                ctx.auxiliary("validity")
                    .compress(v.as_ref(), like_delta.and_then(|d| d.validity()))
            })
            .transpose()?;

        // Fill forward nulls
        let filled = fill_forward(array)?;
        let delta_encoded = match_each_integer_ptype!(parray.ptype(), |$T| {
            PrimitiveArray::from(compress_primitive(filled.as_primitive().typed_data::<$T>()))
        });

        let encoded = ctx
            .named("deltas")
            .compress(&delta_encoded, like_delta.map(|d| d.encoded()))?;

        Ok(DeltaArray::try_new(array.len(), encoded, validity)
            .unwrap()
            .into_array())
    }
}

fn compress_primitive<T: NativePType + Delta + WrappingSub>(array: &[T]) -> Vec<T>
where
    [(); 128 / size_of::<T>()]:,
{
    // How many fastlanes vectors we will process.
    let num_chunks = array.len() / 1024;
    let lanes = T::lanes();

    // Allocate a result array.
    let mut output = Vec::with_capacity(array.len());
    let mut base_scalar = T::default();

    // Loop over all but the last chunk.
    if num_chunks > 0 {
        let mut transposed: [T; 1024] = [T::default(); 1024];
        for i in 0..num_chunks {
            let start_elem = i * 1024;
            let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];
            transpose(chunk, &mut transposed);

            // Always use a base vector of zeros for now, until we split out the bases from deltas
            let mut base = [T::default(); 128 / size_of::<T>()];
            Delta::encode_transposed(&transposed, &mut base, &mut output);
        }
        base_scalar = array[num_chunks * 1024 - 1];
    }

    // To avoid padding, the remainder is encoded with scalar logic.
    let remainder_size = array.len() % 1024;
    if remainder_size > 0 {
        let chunk = &array[array.len() - remainder_size..];
        for next in chunk {
            let diff = next.wrapping_sub(&base_scalar);
            output.push(diff);
            base_scalar = *next;
        }
    }

    output
}

pub fn decompress(array: &DeltaArray) -> VortexResult<PrimitiveArray> {
    let deltas = flatten_primitive(array.encoded())?;
    let decoded = match_each_integer_ptype!(deltas.ptype(), |$T| {
        PrimitiveArray::from_nullable(
            decompress_primitive::<$T>(deltas.typed_data()),
            array.validity().cloned()
        )
    });
    Ok(decoded)
}

fn decompress_primitive<T: NativePType + Delta + WrappingAdd>(array: &[T]) -> Vec<T>
where
    [(); 128 / size_of::<T>()]:,
{
    // How many fastlanes vectors we will process.
    let num_chunks = array.len() / 1024;

    // Allocate a result array.
    let mut output = Vec::with_capacity(array.len());
    let mut base_scalar = T::default();

    // Loop over all the chunks
    if num_chunks > 0 {
        let mut transposed: [T; 1024] = [T::default(); 1024];
        for i in 0..num_chunks {
            let start_elem = i * 1024;
            let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];

            // Always use a base vector of zeros for now, until we split out the bases from deltas
            let mut base = [T::default(); 128 / size_of::<T>()];
            Delta::decode_transposed(chunk, &mut base, &mut transposed);
            untranspose(&transposed, &mut output);
        }
        base_scalar = output[output.len() - 1];
    }
    assert_eq!(output.len() % 1024, 0);

    // To avoid padding, the remainder is encoded with scalar logic.
    let remainder_size = array.len() % 1024;
    if remainder_size > 0 {
        let chunk = &array[num_chunks * 1024..];
        for next_diff in chunk {
            let next = next_diff.wrapping_add(&base_scalar);
            output.push(next);
            base_scalar = next;
        }
    }

    output
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::sync::Arc;

    use vortex::array::Encoding;

    use super::*;

    fn compress_ctx() -> CompressCtx {
        let cfg = CompressConfig::new(HashSet::from([DeltaEncoding.id()]), HashSet::default());
        CompressCtx::new(Arc::new(cfg))
    }

    #[test]
    fn test_compress() {
        let ctx = compress_ctx();
        let compressed = DeltaEncoding {}
            .compress(&PrimitiveArray::from(Vec::from_iter(0..10_000)), None, ctx)
            .unwrap();

        assert_eq!(compressed.encoding().id(), DeltaEncoding.id());
        let delta = compressed.as_any().downcast_ref::<DeltaArray>().unwrap();

        let decompressed = decompress(delta).unwrap();
        let decompressed_slice = decompressed.typed_data::<i32>();
        assert_eq!(decompressed_slice.len(), 10_000);
        for (actual, expected) in decompressed_slice.iter().zip(0..10_000) {
            assert_eq!(actual, &expected);
        }
    }

    #[test]
    fn test_compress_overflow() {
        let ctx = compress_ctx();
        let compressed = ctx
            .compress(
                &PrimitiveArray::from(Vec::from_iter((0..10_000).map(|i| (i % 63) as u8))),
                None,
            )
            .unwrap();
        assert_eq!(compressed.encoding().id(), DeltaEncoding.id());
        _ = compressed.as_any().downcast_ref::<DeltaArray>().unwrap();
    }
}
