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
use std::io::ErrorKind;

use croaring::{Bitmap, Portable};

use vortex::array::{Array, ArrayRef};
use vortex::ptype::PType;
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{RoaringIntArray, RoaringIntEncoding};

impl ArraySerde for RoaringIntArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        let mut data = Vec::new();
        self.bitmap().serialize_into::<Portable>(&mut data);
        ctx.write_slice(data.as_slice())
    }
}

impl EncodingSerde for RoaringIntEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let bitmap_data = ctx.read_slice()?;
        let ptype: PType = ctx
            .schema()
            .try_into()
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        Ok(RoaringIntArray::new(
            Bitmap::try_deserialize::<Portable>(bitmap_data.as_slice())
                .ok_or(io::Error::new(ErrorKind::InvalidData, "invalid bitmap"))?,
            ptype,
        )
        .boxed())
    }
}

#[cfg(test)]
mod test {
    use croaring::Bitmap;

    use vortex::ptype::PType;

    use crate::downcast::DowncastRoaring;
    use crate::serde_tests::test::roundtrip_array;
    use crate::RoaringIntArray;

    #[test]
    fn roundtrip() {
        let arr = RoaringIntArray::new(Bitmap::from_range(245..63000), PType::U32);
        let read_arr = roundtrip_array(arr.as_ref()).unwrap();
        let read_roaring = read_arr.as_roaring_int();
        assert_eq!(arr.ptype(), read_roaring.ptype());
        assert_eq!(arr.bitmap(), read_roaring.bitmap());
    }
}
