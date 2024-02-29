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

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::varbinview::{VarBinViewArray, VarBinViewEncoding, VARBINVIEW_ENCODING};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use rayon::prelude::*;

impl EncodingCompression for VarBinViewEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &VARBINVIEW_ENCODING {
            Some(&(varbinview_compressor as Compressor))
        } else {
            None
        }
    }
}

fn varbinview_compressor(
    array: &dyn Array,
    like: Option<&dyn Array>,
    ctx: CompressCtx,
) -> ArrayRef {
    let varbinview_array = array.as_varbinview();
    let varbinview_like = like.map(|like_array| like_array.as_varbinview());

    VarBinViewArray::new(
        // TODO(robert): Can we compress views? Not right now
        dyn_clone::clone_box(varbinview_array.views()),
        varbinview_like
            .map(|vbvlike| {
                varbinview_array
                    .data()
                    .par_iter()
                    .zip_eq(vbvlike.data())
                    .map(|(d, dlike)| ctx.compress(d.as_ref(), Some(dlike.as_ref())))
                    .collect()
            })
            .unwrap_or_else(|| {
                varbinview_array
                    .data()
                    .par_iter()
                    .map(|d| ctx.compress(d.as_ref(), None))
                    .collect()
            }),
        array.dtype().clone(),
        varbinview_array.validity().map(|v| {
            ctx.compress(
                v.as_ref(),
                varbinview_like
                    .and_then(|vbvlike| vbvlike.validity())
                    .map(|v| v.as_ref()),
            )
        }),
    )
    .boxed()
}
