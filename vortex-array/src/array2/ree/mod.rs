mod compute;

use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

use crate::array2::ArrayCompute;
use crate::array2::ArrayView;
use crate::array2::ScalarAtFn;
use crate::array2::TypedArrayView;
use crate::array2::{Array, ArrayEncoding, ArrayMetadata, ParseArrayMetadata};
use crate::array2::{ArrayData, TypedArrayData};
use crate::impl_encoding;
use crate::scalar::Scalar;

impl_encoding!("vortex.ree", REE);

#[derive(Clone, Debug)]
pub struct REEMetadata {
    length: usize,
    ends_dtype: DType,
}

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
        ArrayData::new(
            &REEEncoding,
            values.dtype().clone(),
            REEMetadata {
                length,
                ends_dtype: ends.dtype().clone(),
            }
            .into_arc(),
            vec![].into(),
            vec![ends, values].into(),
        )
        .as_typed()
    }
}

impl ParseArrayMetadata for REEMetadata {
    fn try_from(metadata: Option<&[u8]>) -> VortexResult<Self> {
        let Some(bytes) = metadata else {
            vortex_bail!("REE metadata is missing")
        };
        todo!()
    }
}

impl ArrayCompute for &dyn REEArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for &dyn REEArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        todo!()
    }
}

impl REEArray for TypedArrayData<REEDef> {
    fn run_ends(&self) -> Array {
        Array::DataRef(
            self.data()
                .children()
                .get(0)
                // FIXME(ngates): where are these assertions made?
                .expect("REEArray should have at least one child"),
        )
    }

    fn values(&self) -> Array {
        todo!()
    }
}

impl REEArray for TypedArrayView<'_, REEDef> {
    fn run_ends(&self) -> Array {
        Array::View(
            self.view()
                .child(0, self.metadata().ends_dtype())
                .expect("REEArray missing ends child"),
        )
    }

    fn values(&self) -> Array {
        Array::View(
            self.view()
                .child(1, self.view().dtype())
                .expect("REEArray missing ends child"),
        )
    }
}

impl FromArrayView for REEView<'_> {
    fn try_from(view: &ArrayView) -> VortexResult<Self> {
        todo!()
    }
}

impl FromArrayData for REEData {
    fn try_from(data: &ArrayData) -> VortexResult<Self> {
        todo!()
    }
}
