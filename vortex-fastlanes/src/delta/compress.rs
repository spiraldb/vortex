use arrayref::array_ref;
use log::debug;

use fastlanez_sys::TryBitPack;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::match_each_integer_ptype;
use vortex::ptype::{NativePType, PType};
use vortex::scalar::ListScalarVec;
use vortex::stats::Stat;

use crate::{DeltaArray, DeltaEncoding};

impl EncodingCompression for DeltaEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive arrays
        let Some(parray) = array.maybe_primitive() else {
            debug!("Skipping Delta: not primitive");
            return None;
        };

        // Only supports ints
        if !parray.ptype().is_int() {
            debug!("Skipping Delta: not int");
            return None;
        }

        debug!("Compressing with Delta");
        Some(&(delta_compressor as Compressor))
    }
}

fn delta_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let parray = array.as_primitive();
    let like_delta = like.map(|l| l.as_any().downcast_ref::<DeltaArray>().unwrap());

    let validity = parray
        .validity()
        .map(|v| ctx.compress(v.as_ref(), like_delta.and_then(|d| d.validity())));

    let delta_encoded = match_each_integer_ptype!(parray.ptype(), |$T| {
        delta_primitive(parray.typed_data::<$T>())
    });

    let encoded = ctx.next_level().compress(
        PrimitiveArray::from_vec(delta_encoded).as_ref(),
        like_delta.map(|d| d.encoded().as_ref()),
    );

    return DeltaArray::try_new(encoded, validity).unwrap().boxed();
}

fn delta_primitive<T: NativePType + TryBitPack>(array: &[T], bit_width: usize) -> Vec<u8> {
    // How many fastlanes vectors we will process.
    let num_chunks = (array.len() + 1023) / 1024;

    // Allocate a result byte array.
    let mut output = Vec::with_capacity(num_chunks * bit_width * 128);

    // Loop over all but the last chunk.
    (0..num_chunks - 1).for_each(|i| {
        let start_elem = i * 1024;
        let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];
        TryBitPack::try_bitpack_into(chunk, bit_width, &mut output).unwrap();
    });

    // Pad the last chunk with zeros to a full 1024 elements.
    let last_chunk_size = array.len() % 1024;
    let mut last_chunk: [T; 1024] = [T::default(); 1024];
    last_chunk[..last_chunk_size].copy_from_slice(&array[array.len() - last_chunk_size..]);
    TryBitPack::try_bitpack_into(&last_chunk, bit_width, &mut output).unwrap();

    output
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use vortex::array::primitive::PrimitiveEncoding;
    use vortex::array::Encoding;

    use super::*;

    #[test]
    fn test_compress() {
        // FIXME(ngates): remove PrimitiveEncoding https://github.com/fulcrum-so/vortex/issues/35
        let cfg = CompressConfig::new(
            HashSet::from([PrimitiveEncoding.id(), DeltaEncoding.id()]),
            HashSet::default(),
        );
        let ctx = CompressCtx::new(&cfg);

        let compressed = ctx.compress(
            &PrimitiveArray::from_vec(Vec::from_iter((0..10_000).map(|i| (i % 63) as u8))),
            None,
        );
        assert_eq!(compressed.encoding().id(), DeltaEncoding.id());
        let bp = compressed.as_any().downcast_ref::<DeltaArray>().unwrap();
        assert_eq!(bp.bit_width(), 6);
    }
}
