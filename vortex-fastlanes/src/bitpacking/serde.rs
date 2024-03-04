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

use std::io;

use vortex::array::{Array, ArrayRef};
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{BitPackedArray, BitPackedEncoding};

impl ArraySerde for BitPackedArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write(self.encoded())?;
        ctx.write_optional_array(self.validity())?;
        ctx.write_optional_array(self.patches())?;
        ctx.write_usize(self.bit_width())?;
        ctx.dtype(self.dtype())?;
        ctx.write_usize(self.len())
    }
}

impl EncodingSerde for BitPackedEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let encoded = ctx.read()?;
        let validity = ctx.read_optional_array()?;
        let patches = ctx.read_optional_array()?;
        let bit_width = ctx.read_usize()?;
        let dtype = ctx.dtype()?;
        let len = ctx.read_usize()?;
        Ok(
            BitPackedArray::try_new(encoded, validity, patches, bit_width, dtype, len)
                .unwrap()
                .boxed(),
        )
    }
}
