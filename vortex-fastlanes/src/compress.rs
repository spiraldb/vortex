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

use arrayref::array_ref;
use log::debug;

use fastlanez_sys::TryBitPack;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::ptype::{NativePType, PType};
use vortex::scalar::ListScalarVec;
use vortex::stats::Stat;

use crate::{BitPackedArray, BitPackedEncoding};

impl EncodingCompression for BitPackedEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive arrays
        let Some(parray) = array.maybe_primitive() else {
            debug!("Skipping BitPacking: not primitive");
            return None;
        };

        // Only supports unsigned ints
        // TODO(ngates): we should add an array that narrows types as much as possible.
        //  e.g. signed -> unsigned, 64 -> 32, etc.
        if !parray.ptype().is_unsigned_int() {
            debug!("Skipping BitPacking: not unsigned int");
            return None;
        }

        // TODO(nates): check if best bit-width is actually narrower than ptype?

        debug!("Compressing with BitPacking");
        Some(&(bitpacked_compressor as Compressor))
    }
}

fn bitpacked_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let parray = array.as_primitive();
    let bit_width_freq = parray
        .stats()
        .get_or_compute_as::<ListScalarVec<usize>>(&Stat::BitWidthFreq)
        .unwrap()
        .0;

    let like_bp = like.map(|l| l.as_any().downcast_ref::<BitPackedArray>().unwrap());

    let bit_width = like_bp
        .map(|bp| bp.bit_width())
        .unwrap_or_else(|| best_bit_width(parray.ptype(), &bit_width_freq));

    return BitPackedArray::try_new(
        bitpack(parray, bit_width as usize),
        parray.validity().map(|v| {
            ctx.compress(
                v.as_ref(),
                like_bp.and_then(|bp| bp.validity().map(|a| a.as_ref())),
            )
        }),
        if num_exceptions(bit_width, &bit_width_freq) > 0 {
            Some(bitpack_patches(parray, bit_width))
        } else {
            None
        },
        bit_width,
        parray.dtype().clone(),
        parray.len(),
    )
    .unwrap()
    .boxed();
}

fn bitpack(parray: &PrimitiveArray, bit_width: usize) -> ArrayRef {
    use PType::*;
    let bytes = match parray.ptype() {
        U8 | U16 | U32 | U64 => bitpack_primitive(parray.buffer().typed_data::<u8>(), bit_width),
        _ => panic!("Unsupported type"),
    };
    // bitpack_primitive(parray.buffer().typed_data::<$T>(), bit_width)
    PrimitiveArray::from_vec(bytes).boxed()
}

fn bitpack_primitive<T: NativePType>(array: &[T], bit_width: usize) -> Vec<u8>
where
    T: TryBitPack,
{
    // How many fastlanes vectors we will process.
    let num_chunks = (array.len() + 1023) / 1024;

    // Allocate a result byte array.
    let mut output = Vec::with_capacity(num_chunks * bit_width * 128);

    // Loop over all but the last chunk.
    (0..num_chunks - 1).into_iter().for_each(|i| {
        let start_elem = i * 1024;
        let chunk: &[T; 1024] = array_ref![array, start_elem, 1024];
        TryBitPack::try_bitpack_into(chunk, bit_width as u8, &mut output).unwrap();
    });

    // Pad the last chunk with zeros to a full 1024 elements.
    let last_chunk_size = array.len() % 1024;
    let mut last_chunk: [T; 1024] = [T::default(); 1024];
    last_chunk[..last_chunk_size].copy_from_slice(&array[array.len() - last_chunk_size..]);
    TryBitPack::try_bitpack_into(&last_chunk, bit_width as u8, &mut output).unwrap();

    output
}

fn bitpack_patches(_parray: &PrimitiveArray, _bit_width: u8) -> ArrayRef {
    todo!("bitpack_patches")
}

/// Assuming exceptions cost 1 value + 1 u32 index, figure out the best bit-width to use.
/// We could try to be clever, but we can never really predict how the exceptions will compress.
fn best_bit_width(ptype: &PType, bit_width_freq: &Vec<usize>) -> u8 {
    let len: usize = bit_width_freq.iter().sum();
    let bytes_per_exception = ptype.byte_width() + 4;

    if bit_width_freq.len() > u8::MAX as usize {
        panic!("Too many bit widths");
    }

    let mut num_packed = 0;
    let mut best_cost = len * bytes_per_exception;
    let mut best_width = 0;
    for (bit_width, freq) in bit_width_freq.iter().enumerate() {
        num_packed += *freq;
        let packed_cost = ((bit_width * len) + 7) / 8;
        let exceptions_cost = (len - num_packed) * bytes_per_exception;
        let cost = exceptions_cost + packed_cost;
        if cost < best_cost {
            best_cost = cost;
            best_width = bit_width;
        }
    }

    best_width as u8
}

fn num_exceptions(bit_width: u8, bit_width_freq: &Vec<usize>) -> usize {
    bit_width_freq[(bit_width + 1) as usize..].iter().sum()
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use vortex::array::primitive::PrimitiveEncoding;
    use vortex::array::Encoding;

    use super::*;

    #[test]
    fn test_best_bit_width() {
        // 10 1-bit values, 20 2-bit, etc.
        let freq = vec![0, 10, 20, 15, 1, 0, 0, 0];
        // 3-bits => (46 * 3) + (8 * 1 * 5) => 178 bits => 23 bytes and zero exceptions
        assert_eq!(best_bit_width(&PType::U8, &freq), 3);
    }

    #[test]
    fn test_compress() {
        // FIXME(ngates): remove PrimitiveEncoding https://github.com/fulcrum-so/vortex/issues/35
        let cfg = CompressConfig::new(
            HashSet::from([PrimitiveEncoding.id(), BitPackedEncoding.id()]),
            HashSet::default(),
        );
        let ctx = CompressCtx::new(&cfg);

        let compressed = ctx.compress(
            &PrimitiveArray::from_vec(Vec::from_iter((0..10_000).map(|i| (i % 63) as u8))),
            None,
        );
        assert_eq!(compressed.encoding().id(), BitPackedEncoding.id());
        let bp = compressed
            .as_any()
            .downcast_ref::<BitPackedArray>()
            .unwrap();
        assert_eq!(bp.bit_width(), 6);
    }
}
