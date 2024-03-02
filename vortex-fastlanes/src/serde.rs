use vortex::serde::{ArraySerde, WriteCtx};

use crate::BitPackedArray;

impl ArraySerde for BitPackedArray {
    fn write(&self, _ctx: &mut WriteCtx) -> std::io::Result<()> {
        todo!()
    }
}
