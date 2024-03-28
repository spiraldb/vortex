use crate::array::primitive::PrimitiveEncoding;
use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::ptype::PType;
use crate::serde::ReadCtx;
use crate::stats::Stats;
use crate::validity::{ArrayValidity, Validity};
use crate::view::TypedArrayView;
use arrow_buffer::Buffer;
use std::any::Any;
use std::io::Cursor;
use std::sync::Arc;
use vortex_error::VortexResult;
use vortex_schema::IntWidth::_32;
use vortex_schema::Nullability::Nullable;
use vortex_schema::Signedness::Signed;
use vortex_schema::{DType, Nullability};

type PrimitiveView<'a> = TypedArrayView<'a, PrimitiveEncoding>;

impl PrimitiveView<'_> {
    pub fn ptype(&self) -> PType {
        let meta = self.view().metadata().expect("Missing metadata");
        let mut cursor = Cursor::new(meta);
        let mut ctx = ReadCtx::new(self.dtype(), &mut cursor);
        let ptype = ctx.ptype().unwrap();
        let _nullability = ctx.nullability().unwrap();
        let _validity = ctx.read_validity().unwrap();
        ptype
    }

    pub fn nullability(&self) -> Nullability {
        let meta = self.view().metadata().expect("Missing metadata");
        let mut cursor = Cursor::new(meta);
        let mut ctx = ReadCtx::new(self.dtype(), &mut cursor);
        let _ptype = ctx.ptype().unwrap();
        let nullability = ctx.nullability().unwrap();
        let _validity = ctx.read_validity().unwrap();
        nullability
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
