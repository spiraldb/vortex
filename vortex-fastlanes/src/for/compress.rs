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

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::match_each_integer_ptype;
use vortex::stats::Stat;

use crate::{FoRArray, FoREncoding};

impl EncodingCompression for FoREncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        // Only support primitive arrays
        let Some(parray) = array.maybe_primitive() else {
            debug!("Skipping FoR: not primitive");
            return None;
        };

        // Only supports integers
        if !parray.ptype().is_int() {
            debug!("Skipping FoR: not int");
            return None;
        }

        // Nothing for us to do if the min is already zero.
        if parray
            .stats()
            .get_or_compute_cast::<i64>(&Stat::Min)
            .unwrap()
            == 0
        {
            debug!("Skipping BitPacking: min is zero");
            return None;
        }

        debug!("Compressing with FoR");
        Some(&(for_compressor as Compressor))
    }
}

fn for_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let parray = array.as_primitive();

    let child = match_each_integer_ptype!(parray.ptype(), |$T| {
        let min = parray.stats().get_or_compute_as::<$T>(&Stat::Min).unwrap();
        // TODO(ngates): check for overflow
        let values = parray.buffer().typed_data::<$T>().iter().map(|v| v - min)
            // TODO(ngates): cast to unsigned
            // .map(|v| v as parray.ptype().to_unsigned()::T)
            .collect_vec();
        PrimitiveArray::from_vec(values)
    });

    // TODO(ngates): remove FoR as a potential encoding from the ctx
    let compressed_child = ctx.compress(
        child.as_ref(),
        like.map(|l| l.as_any().downcast_ref::<FoRArray>().unwrap().child()),
    );
    let reference = parray.stats().get(&Stat::Min).unwrap();
    FoRArray::try_new(compressed_child, reference)
        .unwrap()
        .boxed()
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use vortex::array::primitive::PrimitiveEncoding;
    use vortex::array::Encoding;

    use super::*;

    #[test]
    fn test_compress() {
        let cfg = CompressConfig::new(
            // We need some BitPacking else we will need choose FoR.
            HashSet::from([
                PrimitiveEncoding.id(),
                FoREncoding.id(),
                BitPackedEncoding.id(),
            ]),
            HashSet::default(),
        );
        let ctx = CompressCtx::new(&cfg);

        // Create a range offset by a million
        let array = PrimitiveArray::from_vec((0u32..10_000).map(|v| v + 1_000_000).collect_vec());

        let compressed = ctx.compress(&array, None);
        assert_eq!(compressed.encoding().id(), FoREncoding.id());
        let fa = compressed.as_any().downcast_ref::<FoRArray>().unwrap();
        assert_eq!(fa.reference().try_into(), Ok(1_000_000u32));
    }
}
