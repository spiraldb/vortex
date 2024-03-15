use arrow_array::{ArrayRef as ArrowArrayRef, StructArray as ArrowStructArray};
use arrow_schema::{Field, Fields};
use itertools::Itertools;
use std::sync::Arc;

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::struct_::StructArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_arrow::{as_arrow, AsArrowArray};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{flatten, FlattenFn};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{Scalar, StructScalar};

impl ArrayCompute for StructArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

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

impl AsContiguousFn for StructArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        let mut fields = vec![Vec::new(); self.fields().len()];
        for array in arrays {
            for f in 0..self.fields().len() {
                fields[f].push(array.as_struct().fields()[f].clone())
            }
        }

        Ok(StructArray::new(
            self.names().clone(),
            fields
                .iter()
                .map(|field_arrays| as_contiguous(field_arrays.clone()))
                .try_collect()?,
        )
        .boxed())
    }
}

impl FlattenFn for StructArray {
    fn flatten(&self) -> VortexResult<ArrayRef> {
        Ok(StructArray::new(
            self.names().clone(),
            self.fields()
                .iter()
                .map(|field| flatten(field.as_ref()))
                .try_collect()?,
        )
        .boxed())
    }
}

impl ScalarAtFn for StructArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(StructScalar::new(
            self.dtype.clone(),
            self.fields
                .iter()
                .map(|field| scalar_at(field.as_ref(), index))
                .try_collect()?,
        )
        .into())
    }
}
