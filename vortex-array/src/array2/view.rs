use vortex_error::{vortex_bail, VortexError};

use crate::array2::{ArrayDef, PrimitiveArray};
use crate::serde::ArrayView;

pub struct TypedArrayView<'v, D: ArrayDef> {
    view: ArrayView<'v>,
    metadata: D::Metadata,
}

impl<'v, D: ArrayDef> TypedArrayView<'v, D>
where
    Self: AsRef<D::Array<'v>>,
{
    pub fn view(&'v self) -> &'v ArrayView<'v> {
        &self.view
    }

    pub fn as_array(&self) -> &D::Array<'v> {
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
