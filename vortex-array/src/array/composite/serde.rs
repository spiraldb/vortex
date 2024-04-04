use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;
use vortex_error::VortexResult;
use vortex_flatbuffers::WriteFlatBuffer;
use vortex_schema::DType;

use crate::array::composite::{CompositeArray, CompositeEncoding};
use crate::array::{Array, ArrayRef, OwnedArray};
use crate::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};

impl ArraySerde for CompositeArray {
    fn write(&self, ctx: &mut WriteCtx) -> VortexResult<()> {
        ctx.write_slice(self.metadata().as_slice())?;
        let underlying = self.underlying();
        ctx.dtype(underlying.dtype())?;
        ctx.write(self.underlying())
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        let mut fbb = FlatBufferBuilder::new();
        let dtype = self.underlying().dtype().write_flatbuffer(&mut fbb);
        fbb.finish_minimal(dtype);
        Ok(Some(fbb.finished_data().to_vec()))
    }
}

impl EncodingSerde for CompositeEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> VortexResult<ArrayRef> {
        let DType::Composite(id, _) = *ctx.schema() else {
            panic!("Expected composite schema, found {}", ctx.schema())
        };
        let metadata = ctx.read_slice()?;
        let underling_dtype = ctx.dtype()?;
        let underlying = ctx.with_schema(&underling_dtype).read()?;

        Ok(CompositeArray::new(id, Arc::new(metadata), underlying).into_array())
    }
}
