use std::io;

use codecz::alp::ALPExponents;
use enc::array::{Array, ArrayRef};
use enc::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

use crate::{ALPArray, ALPEncoding};

impl ArraySerde for ALPArray {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()> {
        if let Some(p) = self.patches() {
            ctx.writer().write_all(&[0x01])?;
            ctx.write(p.as_ref())?;
        } else {
            ctx.writer().write_all(&[0x00])?;
        }
        ctx.writer()
            .write_all(&[self.exponents().e, self.exponents().f])?;
        ctx.write(self.encoded())
    }
}

impl EncodingSerde for ALPEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef> {
        let patches_tag = ctx.read_nbytes::<1>()?[0];
        let patches = if patches_tag == 0x01 {
            Some(ctx.read()?)
        } else {
            None
        };
        let exponents = ctx.read_nbytes::<2>()?;
        let encoded = ctx.read()?;
        Ok(ALPArray::new(
            encoded,
            ALPExponents {
                e: exponents[0],
                f: exponents[1],
            },
            patches,
        )
        .boxed())
    }
}
