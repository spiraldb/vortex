use std::mem::size_of;

use arrayref::array_ref;

use fastlanez_sys::{transpose, Delta};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::fill::fill_forward;
use vortex::error::VortexResult;
use vortex::match_each_signed_integer_ptype;
use vortex::ptype::NativePType;
use vortex::stats::Stat;

use crate::{DeltaArray, DeltaEncoding};

impl EncodingCompression for DeltaEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        // Only support primitive arrays
        let parray = array.maybe_primitive()?;

        // Only supports signed ints
        if !parray.ptype().is_signed_int() {
            return None;
        }

        if parray
            .stats()
            .get_or_compute_cast::<i64>(&Stat::Min)
            .unwrap_or(0)
            != 0
        {
            return None;
        }

        Some(self)
        //
        // // For now, only consider delta on sorted arrays
        // if parray
        //     .stats()
        //     .get_or_compute_as::<bool>(&Stat::IsSorted)
        //     .unwrap_or(false)
        // {
        //     return Some(self);
        // }
        // None
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
        let delta_encoded = match_each_signed_integer_ptype!(parray.ptype(), |$T| {
            PrimitiveArray::from(delta_primitive(filled.as_primitive().typed_data::<$T>()))
        });

        let encoded = ctx
            .named("deltas")
            .compress(delta_encoded.as_ref(), like_delta.map(|d| d.encoded()))?;

        Ok(DeltaArray::try_new(array.len(), encoded, validity)
            .unwrap()
            .boxed())
    }
}

fn delta_primitive<T: NativePType + Delta>(array: &[T]) -> Vec<T>
where
    [(); 128 / size_of::<T>()]:,
{
    // How many fastlanes vectors we will process.
    let num_chunks = (array.len() + 1023) / 1024;

    // Allocate a result array.
    let mut output = Vec::with_capacity(array.len());

    // Start with a base vector of zeros.
    let mut base = [T::default(); 128 / size_of::<T>()];

    // Loop over all but the last chunk.
    (0..num_chunks - 1).for_each(|i| {
        let start_elem = i * 1024;
        let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];
        let transposed = transpose(chunk);
        Delta::delta(&transposed, &mut base, &mut output);
    });

    // To avoid padding, the remainder is encoded with scalar logic.
    let mut base_scalar = base[base.len() - 1];
    let last_chunk_size = array.len() % 1024;
    if last_chunk_size > 0 {
        let chunk = &array[array.len() - last_chunk_size..];
        for next in chunk {
            output.push(*next - base_scalar);
            base_scalar = next;
        }
    }

    output
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::sync::Arc;

    use vortex::array::primitive::PrimitiveEncoding;
    use vortex::array::Encoding;

    use super::*;

    fn compress_ctx() -> CompressCtx {
        let cfg = CompressConfig::new(
            HashSet::from([PrimitiveEncoding.id(), DeltaEncoding.id()]),
            HashSet::default(),
        );
        CompressCtx::new(Arc::new(cfg))
    }

    #[test]
    fn test_compress() {
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
