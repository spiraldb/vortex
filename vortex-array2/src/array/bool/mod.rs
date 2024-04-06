mod compute;

use arrow_buffer::{BooleanBuffer, Buffer};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::validity::Validity;
use crate::validity::{ArrayValidity, ValidityMetadata};
use crate::{impl_encoding, IntoArray};
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayMetadata, TryFromArrayMetadata};
use crate::{ArrayView, ToArrayData};
use crate::{ToArray, TypedArrayView};

impl_encoding!("vortex.bool", Bool);

#[derive(Clone, Debug)]
pub struct BoolMetadata {
    validity: ValidityMetadata,
    length: usize,
}

impl BoolMetadata {
    pub fn validity(&self) -> &ValidityMetadata {
        &self.validity
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

// TODO(ngates): I think this could be a struct?
#[allow(clippy::len_without_is_empty)]
pub trait BoolArray {
    fn buffer(&self) -> &Buffer;
    fn len(&self) -> usize;
    fn validity(&self) -> Option<Validity>;
}

impl BoolData {
    pub fn try_new(buffer: BooleanBuffer, validity: Option<Validity>) -> VortexResult<Self> {
        if let Some(v) = &validity {
            assert_eq!(v.len(), buffer.len());
        }
        let dtype = DType::Bool(validity.is_some().into());
        let metadata = BoolMetadata {
            validity: ValidityMetadata::try_from_validity(validity.as_ref(), &dtype)?,
            length: buffer.len(),
        };
        let validity_array = validity.and_then(|v| v.into_array_data());
        Ok(Self::new_unchecked(
            dtype,
            Arc::new(metadata),
            vec![buffer.into_inner()].into(),
            vec![validity_array].into(),
        ))
    }
}

impl BoolArray for BoolData {
    fn buffer(&self) -> &Buffer {
        self.data().buffers().first().unwrap()
    }

    fn len(&self) -> usize {
        self.metadata().len()
    }

    fn validity(&self) -> Option<Validity> {
        self.metadata().validity().to_validity(
            self.metadata().len(),
            self.data().child(0).map(|data| data.to_array()),
        )
    }
}

impl BoolArray for BoolView<'_> {
    fn buffer(&self) -> &Buffer {
        self.view()
            .buffers()
            .first()
            .expect("BoolView must have a single buffer")
    }

    fn len(&self) -> usize {
        self.metadata().len()
    }

    fn validity(&self) -> Option<Validity> {
        self.metadata().validity().to_validity(
            self.metadata().len(),
            self.view()
                .child(0, &Validity::DTYPE)
                .map(|a| a.into_array()),
        )
    }
}

impl TryFromArrayMetadata for BoolMetadata {
    fn try_from_metadata(_metadata: Option<&[u8]>) -> VortexResult<Self> {
        todo!()
    }
}

impl<'v> TryFromArrayView<'v> for BoolView<'v> {
    fn try_from_view(view: &'v ArrayView<'v>) -> VortexResult<Self> {
        // TODO(ngates): validate the view.
        Ok(BoolView::new_unchecked(
            view.clone(),
            BoolMetadata::try_from_metadata(view.metadata())?,
        ))
    }
}

impl TryFromArrayData for BoolData {
    fn try_from_data(data: &ArrayData) -> VortexResult<Self> {
        // TODO(ngates): validate the array data.
        Ok(Self::from_data_unchecked(data.clone()))
    }
}

impl ArrayTrait for &dyn BoolArray {
    fn len(&self) -> usize {
        (**self).len()
    }
}

impl ArrayValidity for &dyn BoolArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().map(|v| v.is_valid(index)).unwrap_or(true)
    }
}

impl ToArrayData for &dyn BoolArray {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}
