use std::mem::size_of;

use arrayref::array_ref;
use fastlanez::{transpose, untranspose_into, Delta};
use num_traits::{WrappingAdd, WrappingSub};
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, Compressor, EncodingCompression};
use vortex::compute::fill::fill_forward;
use vortex::validity::Validity;
use vortex::{Array, IntoArray, OwnedArray};
use vortex_dtype::Nullability;
use vortex_dtype::{match_each_integer_ptype, NativePType};
use vortex_error::VortexResult;

use crate::{DeltaArray, DeltaEncoding};

impl EncodingCompression for DeltaEncoding {
    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = PrimitiveArray::try_from(array).ok()?;

        // Only supports ints
        if !parray.ptype().is_int() {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &Array,
        like: Option<&Array>,
        ctx: Compressor,
    ) -> VortexResult<OwnedArray> {
        let parray = PrimitiveArray::try_from(array)?;
        let like_delta = like.map(|l| DeltaArray::try_from(l).unwrap());

        let validity = ctx.compress_validity(parray.validity())?;

        // Fill forward nulls
        let filled = fill_forward(array)?.flatten_primitive()?;

        // Compress the filled array
        let (bases, deltas) = match_each_integer_ptype!(parray.ptype(), |$T| {
            let (bases, deltas) = compress_primitive(filled.typed_data::<$T>());
            let base_validity = (validity.nullability() != Nullability::NonNullable)
                .then(|| Validity::AllValid)
                .unwrap_or(Validity::NonNullable);
            let delta_validity = (validity.nullability() != Nullability::NonNullable)
                .then(|| Validity::AllValid)
                .unwrap_or(Validity::NonNullable);
            (
                // To preserve nullability, we include Validity
                PrimitiveArray::from_vec(bases, base_validity),
                PrimitiveArray::from_vec(deltas, delta_validity),
            )
        });

        // Recursively compress the bases and deltas
        let bases = ctx.named("bases").compress(
            bases.array(),
            like_delta.as_ref().map(|d| d.bases()).as_ref(),
        )?;
        let deltas = ctx.named("deltas").compress(
            deltas.array(),
            like_delta.as_ref().map(|d| d.deltas()).as_ref(),
        )?;

        DeltaArray::try_new(array.len(), bases, deltas, validity).map(|a| a.into_array())
    }
}

fn compress_primitive<T: NativePType + Delta + WrappingSub>(array: &[T]) -> (Vec<T>, Vec<T>)
where
    [(); 128 / size_of::<T>()]:,
{
    // How many fastlanes vectors we will process.
    let num_chunks = array.len() / 1024;

    // How long each base vector will be.
    let lanes = T::lanes();

    // Allocate result arrays.
    let mut bases = Vec::with_capacity(num_chunks * lanes + 1);
    let mut deltas = Vec::with_capacity(array.len());

    // Loop over all the 1024-element chunks.
    if num_chunks > 0 {
        let mut transposed: [T; 1024] = [T::default(); 1024];
        let mut base = [T::default(); 128 / size_of::<T>()];
        assert_eq!(base.len(), lanes);

        for i in 0..num_chunks {
            let start_elem = i * 1024;
            let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];
            transpose(chunk, &mut transposed);

            // Initialize and store the base vector for each chunk
            base.copy_from_slice(&transposed[0..lanes]);
            bases.extend(base);

            Delta::encode_transposed(&transposed, &mut base, &mut deltas);
        }
    }

    // To avoid padding, the remainder is encoded with scalar logic.
    let remainder_size = array.len() % 1024;
    if remainder_size > 0 {
        let chunk = &array[array.len() - remainder_size..];
        let mut base_scalar = chunk[0];
        bases.push(base_scalar);
        for next in chunk {
            let diff = next.wrapping_sub(&base_scalar);
            deltas.push(diff);
            base_scalar = *next;
        }
    }

    assert_eq!(
        bases.len(),
        num_chunks * lanes + (if remainder_size > 0 { 1 } else { 0 })
    );
    assert_eq!(deltas.len(), array.len());

    (bases, deltas)
}

pub fn decompress(array: DeltaArray) -> VortexResult<PrimitiveArray> {
    let bases = array.bases().flatten_primitive()?;
    let deltas = array.deltas().flatten_primitive()?;
    let decoded = match_each_integer_ptype!(deltas.ptype(), |$T| {
        PrimitiveArray::from_vec(
            decompress_primitive::<$T>(bases.typed_data(), deltas.typed_data()),
            array.validity()
        )
    });
    Ok(decoded)
}

fn decompress_primitive<T: NativePType + Delta + WrappingAdd>(bases: &[T], deltas: &[T]) -> Vec<T>
where
    [(); 128 / size_of::<T>()]:,
{
    // How many fastlanes vectors we will process.
    let num_chunks = deltas.len() / 1024;

    // How long each base vector will be.
    let lanes = T::lanes();

    // Allocate a result array.
    let mut output = Vec::with_capacity(deltas.len());

    // Loop over all the chunks
    if num_chunks > 0 {
        let mut transposed: [T; 1024] = [T::default(); 1024];
        let mut base = [T::default(); 128 / size_of::<T>()];
        assert_eq!(base.len(), lanes);

        for i in 0..num_chunks {
            let start_elem = i * 1024;
            let chunk: &[T; 1024] = array_ref![deltas, start_elem, 1024];

            // Initialize the base vector for this chunk
            base.copy_from_slice(&bases[i * lanes..(i + 1) * lanes]);
            Delta::decode_transposed(chunk, &mut base, &mut transposed);
            untranspose_into(&transposed, &mut output);
        }
    }
    assert_eq!(output.len() % 1024, 0);

    // The remainder was encoded with scalar logic, so we need to scalar decode it.
    let remainder_size = deltas.len() % 1024;
    if remainder_size > 0 {
        let chunk = &deltas[num_chunks * 1024..];
        assert_eq!(bases.len(), num_chunks * lanes + 1);
        let mut base_scalar = bases[num_chunks * lanes];
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
    use vortex::encoding::ArrayEncoding;
    use vortex::Context;

    use super::*;

    fn ctx() -> Context {
        Context::default().with_encoding(&DeltaEncoding)
    }

    #[test]
    fn test_compress() {
        do_roundtrip_test(Vec::from_iter(0..10_000));
    }

    #[test]
    fn test_compress_overflow() {
        do_roundtrip_test(Vec::from_iter(
            (0..10_000).map(|i| (i % (u8::MAX as i32)) as u8),
        ));
    }

    fn do_roundtrip_test<T: NativePType>(input: Vec<T>) {
        let compressed = DeltaEncoding
            .compress(
                PrimitiveArray::from(input.clone()).array(),
                None,
                Compressor::new(&ctx()),
            )
            .unwrap();

        assert_eq!(compressed.encoding().id(), DeltaEncoding.id());
        let delta = DeltaArray::try_from(compressed).unwrap();

        let decompressed = decompress(delta).unwrap();
        let decompressed_slice = decompressed.typed_data::<T>();
        assert_eq!(decompressed_slice.len(), input.len());
        for (actual, expected) in decompressed_slice.iter().zip(input) {
            assert_eq!(actual, &expected);
        }
    }
}
