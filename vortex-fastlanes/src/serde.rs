use vortex::serde::{ArraySerde, WriteCtx};

use crate::{BitPackedArray, FoRArray};

impl ArraySerde for BitPackedArray {
    fn write(&self, _ctx: &mut WriteCtx) -> std::io::Result<()> {
        todo!()
    }
}

impl ArraySerde for FoRArray {
    fn write(&self, _ctx: &mut WriteCtx) -> std::io::Result<()> {
        todo!()
    }
}
