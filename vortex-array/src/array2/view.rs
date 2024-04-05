use vortex_error::{vortex_bail, VortexError};

use crate::array2::data::ArrayData;
use crate::array2::primitive::PrimitiveEncoding;
use crate::array2::{ArrayDef, ArrayMetadata, ParseArrayMetadata, ToArrayData};
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

impl<'v, D: ArrayDef> TryFrom<&'v ArrayView<'v>> for TypedArrayView<'v, D>
where
    D::Metadata: ParseArrayMetadata,
{
    type Error = VortexError;

    fn try_from(view: &'v ArrayView<'v>) -> Result<Self, Self::Error> {
        if view.encoding().id() != D::ID {
            vortex_bail!("Invalid encoding for array")
        }
        let metadata =
            <<D as ArrayDef>::Metadata as ParseArrayMetadata>::try_from(view.metadata())?;
        Ok(Self {
            view: view.clone(),
            metadata,
        })
    }
}

pub trait ArrayChildren {
    fn child_array_data(&self) -> Vec<ArrayData>;
}

impl<'v, D: ArrayDef> ToArrayData for TypedArrayView<'v, D>
where
    Self: ArrayChildren,
{
    fn to_data(&self) -> ArrayData {
        // TODO(ngates): how do we get the child types? I guess we could walk?

        ArrayData::new(
            // FIXME(ngates): encoding ref.
            &PrimitiveEncoding,
            // self.view().encoding(),
            self.view().dtype().clone(),
            self.metadata().to_arc(),
            self.view().buffers().to_vec().into(),
            self.child_array_data().into(),
        )
    }
}
