use arrow_buffer::Buffer;
use vortex_error::{vortex_err, VortexResult};
use vortex_schema::DType;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::validity::{Validity, ValidityView};
use crate::array::PrimitiveArray;
use crate::ptype::{NativePType, PType};
use crate::serde::ArrayView;

#[derive(Debug)]
pub struct PrimitiveView<'a> {
    view: &'a ArrayView<'a>,
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
            view,
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
    fn dtype(&self) -> &DType {
        self.view.dtype()
    }

    fn ptype(&self) -> PType {
        self.ptype
    }

    fn validity_view(&self) -> Option<ValidityView> {
        todo!()
    }

    fn buffer(&self) -> &Buffer {
        self.buffer
    }

    fn to_primitive(&self) -> PrimitiveArray {
        PrimitiveArray::new(
            self.ptype(),
            self.buffer.clone(),
            self.validity.as_ref().map(|v| v.to_validity()),
        )
    }
}
