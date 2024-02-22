use std::io;
use std::io::Read;

use croaring::{Bitmap, Portable};

use enc::array::{Array, ArrayRef};
use enc::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{RoaringBoolArray, RoaringBoolEncoding};

impl ArraySerde for RoaringBoolArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        ctx.write_usize(self.len())?;
        let mut data = Vec::new();
        self.bitmap().serialize_into::<Portable>(&mut data);
        ctx.write_usize(data.len())?;
        ctx.writer().write_all(data.as_slice())
    }
}

impl EncodingSerde for RoaringBoolEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let len = ctx.read_usize()?;
        let bitmap_len = ctx.read_usize()?;
        let mut bitmap_data = Vec::<u8>::with_capacity(bitmap_len);
        ctx.reader()
            .take(bitmap_len as u64)
            .read_to_end(&mut bitmap_data)?;
        Ok(RoaringBoolArray::new(
            Bitmap::try_deserialize::<Portable>(bitmap_data.as_slice()).unwrap(),
            len,
        )
        .boxed())
    }
}
