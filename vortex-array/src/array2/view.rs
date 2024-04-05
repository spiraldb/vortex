use std::sync::Arc;

use vortex_error::{vortex_bail, VortexError};

use crate::array2::data::ArrayData;
use crate::array2::{ArrayDef, ArrayMetadata, ToArrayData};
use crate::serde::ArrayView;

pub struct TypedArrayView<'v, D: ArrayDef> {
    view: ArrayView<'v>,
    metadata: D::Metadata,
}

impl<'v, D: ArrayDef> TypedArrayView<'v, D> {
    pub fn metadata(&self) -> &D::Metadata {
        &self.metadata
    }

    pub fn view(&'v self) -> &'v ArrayView<'v> {
        &self.view
    }

    pub fn as_array(&self) -> &D::Array<'v>
    where
        Self: AsRef<D::Array<'v>>,
    {
        self.as_ref()
    }
}

impl<'v, D: ArrayDef> TryFrom<ArrayView<'v>> for TypedArrayView<'v, D>
where
    D::Metadata: TryFrom<Option<&'v [u8]>, Error = VortexError>,
{
    type Error = VortexError;

    fn try_from(view: ArrayView<'v>) -> Result<Self, Self::Error> {
        if view.encoding().id() != D::ID {
            vortex_bail!("Invalid encoding for array")
        }
        let metadata = D::Metadata::try_from(view.metadata())?;
        Ok(Self { view, metadata })
    }
}

impl<'v, D: ArrayDef> ToArrayData for TypedArrayView<'v, D> {
    fn to_array_data(&self) -> ArrayData {
        // TODO(ngates): how do we get the child types? I guess we could walk?

        ArrayData::new(
            self.view().encoding(),
            self.view().dtype().clone(),
            self.metadata().to_arc(),
            self.view().buffers().to_vec().into(),
            self.as_array()
            self.view().children().to_vec().into(),
        )
    }
}
