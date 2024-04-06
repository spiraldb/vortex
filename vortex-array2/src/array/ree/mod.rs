mod compute;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::impl_encoding;
use crate::validity::ArrayValidity;
use crate::{Array, ArrayMetadata, TryFromArrayMetadata};
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayView, ToArrayData};
use crate::{IntoArray, TypedArrayView};

impl_encoding!("vortex.ree", REE);

#[derive(Clone, Debug)]
pub struct REEMetadata {
    length: usize,
    ends_dtype: DType,
}

#[allow(clippy::len_without_is_empty)]
impl REEMetadata {
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn ends_dtype(&self) -> &DType {
        &self.ends_dtype
    }
}

pub trait REEArray {
    fn run_ends(&self) -> Array;
    fn values(&self) -> Array;
}

impl REEData {
    pub fn new(ends: ArrayData, values: ArrayData, length: usize) -> Self {
        ArrayData::try_new(
            &REEEncoding,
            values.dtype().clone(),
            REEMetadata {
                length,
                ends_dtype: ends.dtype().clone(),
            }
            .into_arc(),
            vec![].into(),
            vec![Some(ends), Some(values)].into(),
        )
        .unwrap()
        .try_into()
        .unwrap()
    }
}

impl REEArray for REEData {
    fn run_ends(&self) -> Array {
        Array::DataRef(self.data().child(0).unwrap())
    }

    fn values(&self) -> Array {
        Array::DataRef(self.data().child(1).unwrap())
    }
}

impl REEArray for REEView<'_> {
    fn run_ends(&self) -> Array {
        self.view()
            .child(0, self.metadata().ends_dtype())
            .unwrap()
            .into_array()
    }

    fn values(&self) -> Array {
        self.view()
            .child(1, self.view().dtype())
            .unwrap()
            .into_array()
    }
}

impl TryFromArrayMetadata for REEMetadata {
    fn try_from_metadata(_metadata: Option<&[u8]>) -> VortexResult<Self> {
        todo!()
    }
}

impl<'v> TryFromArrayView<'v> for REEView<'v> {
    fn try_from_view(_view: &'v ArrayView<'v>) -> VortexResult<Self> {
        todo!()
    }
}

impl TryFromArrayData for REEData {
    fn try_from_data(_data: &ArrayData) -> VortexResult<Self> {
        todo!()
    }
}

impl ArrayTrait for &dyn REEArray {
    fn len(&self) -> usize {
        todo!()
    }
}

impl ArrayValidity for &dyn REEArray {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }
}

impl ToArrayData for &dyn REEArray {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}
