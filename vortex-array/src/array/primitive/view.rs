use std::any::Any;
use std::sync::Arc;

use arrow_buffer::Buffer;
use vortex_error::{vortex_err, VortexResult};
use vortex_schema::DType;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::{Array, ArrayRef, PrimitiveArray};
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::ptype::{NativePType, PType};
use crate::serde::ArrayView;
use crate::stats::Stats;
use crate::validity::OwnedValidity;
use crate::validity::{Validity, ValidityView};
use crate::view::ToOwnedView;
use crate::ArrayWalker;

#[derive(Debug)]
pub struct PrimitiveView<'a> {
    ptype: PType,
    buffer: &'a Buffer,
    validity: Option<ValidityView<'a>>,
}

impl<'a> PrimitiveView<'a> {
    pub fn try_new(view: &'a ArrayView<'a>) -> VortexResult<Self> {
        // TODO(ngates): validate the number of buffers / children. We could even extract them?
        let ptype = PType::try_from(view.dtype())?;
        let buffer = view
            .buffers()
            .first()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Missing primitive buffer"))?;
        let validity = view.child(0, &Validity::DTYPE).map(ValidityView::from);

        Ok(Self {
            ptype,
            buffer,
            validity,
        })
    }

    pub fn ptype(&self) -> PType {
        self.ptype
    }

    pub(crate) fn as_trait<T: NativePType>(&self) -> &dyn PrimitiveTrait<T> {
        assert_eq!(self.ptype, T::PTYPE);
        self
    }
}

impl<'a, T: NativePType> PrimitiveTrait<T> for PrimitiveView<'a> {
    fn ptype(&self) -> PType {
        self.ptype
    }

    fn buffer(&self) -> &Buffer {
        self.buffer
    }

    fn to_primitive(&self) -> PrimitiveArray {
        PrimitiveArray::new(
            self.ptype(),
            self.buffer.clone(),
            self.validity.to_owned_view(),
        )
    }
}

impl<'a> OwnedValidity for PrimitiveView<'a> {
    fn validity(&self) -> Option<ValidityView<'a>> {
        self.validity.clone()
    }
}

impl Array for PrimitiveView<'_> {
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
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn dtype(&self) -> &DType {
        todo!()
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn encoding(&self) -> EncodingRef {
        todo!()
    }

    fn nbytes(&self) -> usize {
        todo!()
    }

    fn with_compute_mut(
        &self,
        _f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        todo!()
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayDisplay for PrimitiveView<'_> {
    fn fmt(&self, _fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}
