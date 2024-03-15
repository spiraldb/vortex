use crate::array::struct_::StructArray;
use crate::arrow::as_arrow::{as_arrow, AsArrowArray};
use crate::error::VortexResult;
use arrow_array::{ArrayRef as ArrowArrayRef, StructArray as ArrowStructArray};
use arrow_schema::{Field, Fields};
use itertools::Itertools;
use std::sync::Arc;

impl AsArrowArray for StructArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        let arrow_fields: Fields = self
            .names()
            .iter()
            .zip(self.field_dtypes())
            .map(|(name, dtype)| Field::new(name.as_str(), dtype.into(), dtype.is_nullable()))
            .map(Arc::new)
            .collect();

        let field_arrays: Vec<ArrowArrayRef> = self
            .fields()
            .iter()
            .map(|f| as_arrow(f.as_ref()))
            .try_collect()?;

        Ok(Arc::new(ArrowStructArray::new(
            arrow_fields,
            field_arrays,
            None,
        )))
    }
}
