mod compute;

use serde::{Deserialize, Serialize};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::impl_encoding;
use crate::stats::{ArrayStatistics, Statistics};
use crate::validity::ArrayValidity;
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{Array, ArrayMetadata};
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayView, ToArrayData};

impl_encoding!("vortex.ree", REE);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct REEMetadata {
    length: usize,
    ends_dtype: DType,
}

pub struct REEArray<'a> {
    dtype: &'a DType,
    values: Array<'a>,
    run_ends: Array<'a>,
    length: usize,
    statistics: &'a dyn Statistics,
}

impl REEArray<'_> {
    pub fn values(&self) -> &Array {
        &self.values
    }

    pub fn run_ends(&self) -> &Array {
        &self.run_ends
    }
}

impl REEData {
    pub fn try_new(ends: ArrayData, values: ArrayData, length: usize) -> VortexResult<Self> {
        Ok(Self::new_unchecked(
            values.dtype().clone(),
            Arc::new(REEMetadata {
                length,
                ends_dtype: ends.dtype().clone(),
            }),
            vec![].into(),
            vec![Some(ends), Some(values)].into(),
        ))
    }
}

impl<'v> TryFromArrayParts<'v, REEMetadata> for REEArray<'v> {
    fn try_from_parts(parts: &'v dyn ArrayParts, metadata: &'v REEMetadata) -> VortexResult<Self> {
        Ok(REEArray {
            dtype: parts.dtype(),
            values: parts
                .child(0, parts.dtype())
                .ok_or_else(|| vortex_err!("REEArray missing values"))?,
            run_ends: parts
                .child(1, &metadata.ends_dtype)
                .ok_or_else(|| vortex_err!("REEArray missing run_ends"))?,
            length: metadata.length,
            statistics: parts.statistics(),
        })
    }
}

impl ArrayTrait for REEArray<'_> {
    fn dtype(&self) -> &DType {
        self.values.dtype()
    }

    fn len(&self) -> usize {
        self.length
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

impl AcceptArrayVisitor for REEArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("values", self.values())?;
        visitor.visit_child("run_ends", self.run_ends())
    }
}

impl ArrayStatistics for REEArray<'_> {
    fn statistics(&self) -> &dyn Statistics {
        self.statistics
    }
}
