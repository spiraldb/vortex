use crate::array2::{ArrayMetadata, ArrayView};
use vortex_error::VortexResult;

#[allow(dead_code)]
#[derive(Debug)]
pub struct TypedArrayView<'view, M> {
    view: ArrayView<'view>,
    metadata: M,
}

impl<'view, M> TypedArrayView<'view, M>
where
    M: ArrayMetadata,
{
    pub fn try_new(view: &ArrayView<'view>) -> VortexResult<Self> {
        Ok(Self {
            view: view.clone(),
            metadata: M::try_from_bytes(view.metadata(), &view.dtype())?,
        })
    }

    pub fn metadata(&self) -> &M {
        &self.metadata
    }

    pub fn view(&self) -> &ArrayView<'view> {
        &self.view
    }
}
