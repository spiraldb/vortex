mod compute;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::impl_encoding;
use crate::validity::ArrayValidity;
use crate::{Array, ArrayMetadata};
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayView, ToArrayData};

impl_encoding!("vortex.ree", REE);

#[derive(Clone, Debug)]
pub struct REEMetadata {
    length: usize,
    ends_dtype: DType,
}

impl TryParseArrayMetadata for REEMetadata {
    fn try_parse_metadata(_metadata: Option<&[u8]>) -> VortexResult<Self> {
        todo!()
    }
}

pub struct REEArray<'a> {
    dtype: &'a DType,
    values: Array<'a>,
    run_ends: Array<'a>,
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

impl<'v> TryFromArrayParts<'v, REEMetadata> for REEArray<'v> {
    fn try_from_parts(
        parts: &'v dyn ArrayParts<'v>,
        metadata: &'v REEMetadata,
    ) -> VortexResult<Self> {
        Ok(REEArray {
            dtype: parts.dtype(),
            values: parts
                .child(0, parts.dtype())
                .ok_or_else(|| vortex_err!("REEArray missing values"))?,
            run_ends: parts
                .child(1, &metadata.ends_dtype)
                .ok_or_else(|| vortex_err!("REEArray missing run_ends"))?,
        })
    }
}

impl ArrayTrait for REEArray<'_> {
    fn dtype(&self) -> &DType {
        self.values.dtype()
    }

    fn len(&self) -> usize {
        todo!()
    }
}

impl ArrayValidity for REEArray<'_> {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }
}

impl ToArrayData for REEArray<'_> {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}
