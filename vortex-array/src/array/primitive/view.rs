use crate::array::primitive::PrimitiveEncoding;
use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::ptype::PType;
use crate::serde::ReadCtx;
use crate::stats::Stats;
use crate::validity::{ArrayValidity, Validity};
use crate::view::{ArrayMetadata, ArrayView, ArrayViewVTable, TypedArrayView};
use arrow_buffer::Buffer;
use std::any::Any;
use std::io::Cursor;
use std::sync::Arc;
use vortex_error::VortexResult;
use vortex_schema::IntWidth::_32;
use vortex_schema::Nullability::Nullable;
use vortex_schema::Signedness::Signed;
use vortex_schema::{DType, Nullability};

#[allow(dead_code)]
#[derive(Debug)]
pub struct PrimitiveMetadata {
    ptype: PType,
    nullability: Nullability,
}

impl<'a> ArrayMetadata<'a> for PrimitiveMetadata {
    fn from_bytes(bytes: Option<&'a [u8]>) -> VortexResult<Self> {
        let mut cursor = Cursor::new(bytes.expect("Missing metadata"));
        let mut ctx = ReadCtx::new(&DType::Int(_32, Signed, Nullable), &mut cursor);
        let ptype = ctx.ptype()?;
        let nullability = ctx.nullability()?;
        Ok(PrimitiveMetadata { ptype, nullability })
    }
}

impl<'view> ArrayViewVTable<'view> for PrimitiveEncoding {
    fn to_array(&self, view: &ArrayView<'view>) -> VortexResult<ArrayRef> {
        let p = PrimitiveView::try_new(view)?;
        Ok(p.to_array())
    }

    fn len(&self, view: &ArrayView<'view>) -> VortexResult<usize> {
        Ok(PrimitiveView::try_new(view)?.len())
    }
}

type PrimitiveView<'a> = TypedArrayView<'a, PrimitiveMetadata>;

impl PrimitiveView<'_> {
    pub fn ptype(&self) -> PType {
        self.metadata().ptype
    }

    pub fn nullability(&self) -> Nullability {
        self.metadata().nullability
    }

    pub fn buffer(&self) -> &Buffer {
        self.view().buffers().first().expect("Missing buffer")
    }
}

impl<'a> ArrayCompute for PrimitiveView<'a> {}

impl<'a> ArrayValidity for PrimitiveView<'a> {
    fn validity(&self) -> Option<Validity> {
        todo!()
    }
}

impl<'a> ArrayDisplay for PrimitiveView<'a> {
    fn fmt(&self, _fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}

impl<'a> Array for PrimitiveView<'a> {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        todo!()
    }

    fn to_array(&self) -> ArrayRef {
        todo!()
    }

    fn into_array(self) -> ArrayRef {
        todo!()
    }

    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn dtype(&self) -> &DType {
        &DType::Int(_32, Signed, Nullable)
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        todo!()
    }

    fn encoding(&self) -> EncodingRef {
        todo!()
    }

    fn nbytes(&self) -> usize {
        todo!()
    }
}
