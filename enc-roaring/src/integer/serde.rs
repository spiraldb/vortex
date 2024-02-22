use std::io;
use std::io::{ErrorKind, Read};

use croaring::{Bitmap, Portable};

use enc::array::{Array, ArrayRef};
use enc::ptype::PType;
use enc::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{RoaringIntArray, RoaringIntEncoding};

impl ArraySerde for RoaringIntArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        let mut data = Vec::new();
        self.bitmap().serialize_into::<Portable>(&mut data);
        ctx.write_usize(data.len())?;
        ctx.writer().write_all(data.as_slice())
    }
}

impl EncodingSerde for RoaringIntEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let bitmap_len = ctx.read_usize()?;
        let mut bitmap_data = Vec::<u8>::with_capacity(bitmap_len);
        ctx.reader()
            .take(bitmap_len as u64)
            .read_to_end(&mut bitmap_data)?;
        let ptype: PType = ctx
            .schema()
            .try_into()
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        Ok(RoaringIntArray::new(
            Bitmap::try_deserialize::<Portable>(bitmap_data.as_slice()).unwrap(),
            ptype,
        )
        .boxed())
    }
}
