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

use itertools::Itertools;
use log::debug;

use codecz::ffor;
use codecz::ffor::{FforEncoded, SupportsFFoR};
use vortex::array::{Array, ArrayRef};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::sparse::SparseArray;
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::match_each_integer_ptype;
use vortex::ptype::{NativePType, PType};
use vortex::scalar::{ListScalarVec, NullableScalar, Scalar};
use vortex::stats::Stat;

use crate::{GCDArray, GCDEncoding};

impl EncodingCompression for GCDEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive arrays
        let Some(parray) = array.maybe_primitive() else {
            debug!("Skipping GCD: not primitive");
            return None;
        };

        // Only supports unsigned ints
        // TODO(ngates): cast ints!
        if !parray.ptype().is_int() {
            debug!("Skipping GCD: only supports integer types");
            return None;
        }

        if parray
            .stats()
            .get_or_compute_cast::<i64>(&Stat::Min)
            .unwrap()
            < 0
        {
            debug!("Skipping GCD: only supports positive integers");
            return None;
        }

        let ctz_freq = parray
            .stats()
            .get_or_compute_as::<ListScalarVec<usize>>(&Stat::TrailingZerosFreq)
            .unwrap()
            .0;
        if best_shift(&ctz_freq) == 0 {
            debug!("Skipping GCD: no trailing zeros");
            return None;
        }

        debug!("Compressing with GCD");
        Some(&(gcd_compressor as Compressor))
    }
}

fn gcd_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let like_gcd = like.map(|like_array| like_array.as_any().downcast_ref::<GCDArray>().unwrap());
    let parray = array.as_primitive();

    let ctz_freq = parray
        .stats()
        .get_or_compute_as::<ListScalarVec<usize>>(&Stat::TrailingZerosFreq)
        .unwrap()
        .0;
    let shift = best_shift(&ctz_freq);

    let shifted = match_each_integer_ptype!(parray.ptype(), |$T| {
    PrimitiveArray::from_vec(parray.buffer().typed_data::<$T>()
            .iter()
            .map(|v| v >> shift)
            .collect_vec())
        });

    GCDArray::new(ctx.compress(&shifted, like_gcd.map(|g| g.shifted())), shift).boxed()
}

fn best_shift(ctz_freq: &[usize]) -> u8 {
    let mut shift = 0;
    for &freq in ctz_freq.iter() {
        if freq > 0 {
            break;
        }
        shift += 1;
    }
    shift
}
