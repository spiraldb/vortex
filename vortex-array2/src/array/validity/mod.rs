use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::compute::ArrayCompute;
use crate::impl_encoding;
use crate::TypedArrayView;
use crate::{Array, ArrayMetadata, TryFromArrayMetadata};
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayView, ToArrayData};
use crate::{IntoArray, ToArray};

impl_encoding!("vortex.ree", Validity);

#[derive(Clone, Debug)]
pub enum ValidityMetadata {
    Valid(usize),
    Invalid(usize),
    Array,
}

/// TODO(ngates): I'm not sure validity actually has to be an array itself? I think it could just be
///  something that's returned from ArrayValidity.
pub enum Validity<'v> {
    Valid(usize),
    Invalid(usize),
    Array(Array<'v>),
}

impl Validity<'_> {
    pub const DTYPE: DType = DType::Bool(Nullability::NonNullable);
}

pub trait ValidityArray {
    fn validity(&self) -> Validity;
}

impl ValidityData {
    pub fn new(validity: Validity) -> Self {
        let (meta, children) = match validity {
            Validity::Valid(l) => (ValidityMetadata::Valid(l), vec![]),
            Validity::Invalid(l) => (ValidityMetadata::Invalid(l), vec![]),
            Validity::Array(a) => (ValidityMetadata::Array, vec![Some(a.to_array_data())]),
        };

        ArrayData::try_new(
            &ValidityEncoding,
            Validity::DTYPE,
            meta.into_arc(),
            vec![].into(),
            children.into(),
        )
        .unwrap()
        .try_into()
        .unwrap()
    }
}

impl ValidityArray for ValidityData {
    fn validity(&self) -> Validity {
        match self.metadata() {
            ValidityMetadata::Valid(l) => Validity::Valid(*l),
            ValidityMetadata::Invalid(l) => Validity::Invalid(*l),
            ValidityMetadata::Array => Validity::Array(self.data().child(0).unwrap().to_array()),
        }
    }
}

impl ValidityArray for ValidityView<'_> {
    fn validity(&self) -> Validity {
        match self.metadata() {
            ValidityMetadata::Valid(l) => Validity::Valid(*l),
            ValidityMetadata::Invalid(l) => Validity::Invalid(*l),
            ValidityMetadata::Array => {
                Validity::Array(self.view().child(0, &Validity::DTYPE).unwrap().into_array())
            }
        }
    }
}

impl TryFromArrayMetadata for ValidityMetadata {
    fn try_from_metadata(metadata: Option<&[u8]>) -> VortexResult<Self> {
        let Some(_bytes) = metadata else {
            vortex_bail!("Validity metadata is missing")
        };
        todo!()
    }
}

impl<'v> TryFromArrayView<'v> for ValidityView<'v> {
    fn try_from_view(_view: &'v ArrayView<'v>) -> VortexResult<Self> {
        todo!()
    }
}

impl TryFromArrayData for ValidityData {
    fn try_from_data(_data: &ArrayData) -> VortexResult<Self> {
        todo!()
    }
}

impl ArrayTrait for &dyn ValidityArray {
    fn len(&self) -> usize {
        todo!()
    }
}

impl ToArrayData for &dyn ValidityArray {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}

impl ArrayCompute for &dyn ValidityArray {}
