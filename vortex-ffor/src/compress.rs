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

use log::debug;

use crate::downcast::DowncastFFOR;
use codecz::ffor;
use codecz::ffor::{FforEncoded, SupportsFFoR};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::match_each_integer_ptype;
use vortex::ptype::NativePType;
use vortex::scalar::{ListScalarVec, NullableScalar, Scalar};
use vortex::stats::Stat;

use crate::ffor::{FFORArray, FFoREncoding};

impl EncodingCompression for FFoREncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive arrays
        let Some(parray) = array.maybe_primitive() else {
            debug!("Skipping FFoR: not primitive");
            return None;
        };

        // Only supports ints
        if !parray.ptype().is_int() {
            debug!("Skipping FFoR: only supports integer types");
            return None;
        }

        debug!("Compressing with FFoR");
        Some(&(ffor_compressor as Compressor))
    }
}

// FFOR and other bitpacking algorithms are essentially the "terminal"
// lightweight encodings for integers, as the output is essentially an array
// of opaque bytes. At that point, the only available schemes are general-purpose
// compression algorithms, which we would apply at the file level instead (if at all)
fn ffor_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let like_ffor = like.map(|like_array| like_array.as_ffor());
    let parray = array.as_primitive();

    let (encoded, patches, min_val, num_bits) = like_ffor
        .map(|ffor_like| ffor_encode_like_parts(parray, ffor_like.num_bits()))
        .unwrap_or_else(|| ffor_encode_parts(parray));

    let min_val = if parray.validity().is_some() {
        NullableScalar::some(min_val).boxed()
    } else {
        min_val
    };

    FFORArray::new(
        encoded,
        parray.validity().cloned(),
        patches.map(|p| {
            ctx.next_level().compress(
                p.as_ref(),
                like_ffor.and_then(|lf| lf.patches()).map(|p| p.as_ref()),
            )
        }),
        min_val,
        num_bits,
        parray.len(),
    )
    .boxed()
}

pub fn ffor_encode(parray: &PrimitiveArray) -> FFORArray {
    let (encoded, patches, min_val, num_bits) = ffor_encode_parts(parray);
    FFORArray::new(
        encoded,
        parray.validity().cloned(),
        patches,
        min_val,
        num_bits,
        parray.len(),
    )
}

fn ffor_encode_parts(parray: &PrimitiveArray) -> (ArrayRef, Option<ArrayRef>, Box<dyn Scalar>, u8) {
    let min_val_scalar = parray.stats().get_or_compute(&Stat::Min).unwrap();
    let max_val_scalar = parray.stats().get_or_compute(&Stat::Max).unwrap();
    let bit_widths = parray
        .stats()
        .get_or_compute_as::<ListScalarVec<u64>>(&Stat::BitWidthFreq)
        .unwrap()
        .0;
    match_each_integer_ptype!(parray.ptype(), |$T| {
        let min_val: $T = min_val_scalar.as_ref().try_into().unwrap();
        let max_val: $T = max_val_scalar.as_ref().try_into().unwrap();
        let num_bits = codecz::ffor::find_best_bit_width::<$T>(bit_widths.as_slice(), min_val, max_val).unwrap();
        ffor_encode_primitive(parray.buffer().typed_data::<$T>(), num_bits, min_val)
    })
}

fn ffor_encode_like_parts(
    parray: &PrimitiveArray,
    num_bits: u8,
) -> (ArrayRef, Option<ArrayRef>, Box<dyn Scalar>, u8) {
    let min_val_scalar = parray.stats().get_or_compute(&Stat::Min).unwrap();
    match_each_integer_ptype!(parray.ptype(), |$T| {
        let min_val: $T = min_val_scalar.as_ref().try_into().unwrap();
        ffor_encode_primitive(parray.buffer().typed_data::<$T>(), num_bits, min_val)
    })
}

fn ffor_encode_primitive<T: SupportsFFoR + NativePType>(
    values: &[T],
    num_bits: u8,
    min_val: T,
) -> (ArrayRef, Option<ArrayRef>, Box<dyn Scalar>, u8)
where
    Box<dyn Scalar>: From<T>,
{
    // TODO: actually handle CodecErrors instead of blindly unwrapping
    let FforEncoded {
        buf,
        num_exceptions,
    } = ffor::encode::<T>(values, num_bits, min_val).unwrap();
    let bytes_array = PrimitiveArray::from_vec_in(buf);

    let patches = if num_exceptions == 0 {
        None
    } else {
        let (patch_values, patch_indices) =
            ffor::collect_exceptions(values, num_bits, min_val, num_exceptions).unwrap();
        let patch_indices = codecz::utils::into_u32_vec(&patch_indices, num_exceptions);
        Some(
            SparseArray::new(
                PrimitiveArray::from_vec_in(patch_indices).boxed(),
                PrimitiveArray::from_vec_in(patch_values).boxed(),
                values.len(),
            )
            .boxed(),
        )
    };

    (bytes_array.boxed(), patches, min_val.into(), num_bits)
}
