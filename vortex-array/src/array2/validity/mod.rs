use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::array2::TypedArrayView;
use crate::array2::{Array, ArrayEncoding, ArrayMetadata, TryFromArrayMetadata};
use crate::array2::{ArrayCompute, IntoArray, ToArray};
use crate::array2::{ArrayData, TypedArrayData};
use crate::array2::{ArrayView, ToArrayData};
use crate::impl_encoding;

impl_encoding!("vortex.ree", Validity);

pub trait ArrayValidity {
    fn is_valid(&self, index: usize) -> bool;
}

#[derive(Clone, Debug)]
pub enum ValidityMetadata {
    Valid(usize),
    Invalid(usize),
    Array,
}

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
            Validity::Array(a) => (ValidityMetadata::Array, vec![a.to_array_data()]),
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
            ValidityMetadata::Array => {
                Validity::Array(self.data().children().first().unwrap().to_array())
            }
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
        let Some(bytes) = metadata else {
            vortex_bail!("Validity metadata is missing")
        };
        todo!()
    }
}

impl<'v> TryFromArrayView<'v> for ValidityView<'v> {
    fn try_from_view(view: &'v ArrayView<'v>) -> VortexResult<Self> {
        todo!()
    }
}

impl TryFromArrayData for ValidityData {
    fn try_from_data(data: &ArrayData) -> VortexResult<Self> {
        todo!()
    }
}

impl ArrayTrait for &dyn ValidityArray {
    fn len(&self) -> usize {
        todo!()
    }
}

impl ArrayValidity for &dyn ValidityArray {
    fn is_valid(&self, index: usize) -> bool {
        todo!()
    }
}

impl ToArrayData for &dyn ValidityArray {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}

impl ArrayCompute for &dyn ValidityArray {}
