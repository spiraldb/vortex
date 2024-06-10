use std::sync::Arc;

use arrow_array::{
    Array as ArrowArray, ArrayRef as ArrowArrayRef, StructArray as ArrowStructArray,
};
use arrow_schema::{Field, Fields};
use itertools::Itertools;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::r#struct::StructArray;
use crate::compute::as_arrow::{as_arrow, AsArrowArray};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::slice::{slice, SliceFn};
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
use crate::{Array, ArrayDType, IntoArray};

impl ArrayCompute for StructArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl AsArrowArray for StructArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        let field_arrays: Vec<ArrowArrayRef> =
            self.children().map(|f| as_arrow(&f)).try_collect()?;

        let arrow_fields: Fields = self
            .names()
            .iter()
            .zip(field_arrays.iter())
            .zip(self.dtypes().iter())
            .map(|((name, arrow_field), vortex_field)| {
                Field::new(
                    &**name,
                    arrow_field.data_type().clone(),
                    vortex_field.is_nullable(),
                )
            })
            .map(Arc::new)
            .collect();

        Ok(Arc::new(ArrowStructArray::new(
            arrow_fields,
            field_arrays,
            None,
        )))
    }
}

impl ScalarAtFn for StructArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(Scalar::r#struct(
            self.dtype().clone(),
            self.children()
                .map(|field| scalar_at(&field, index).map(|s| s.into_value()))
                .try_collect()?,
        ))
    }
}

impl TakeFn for StructArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        Self::try_new(
            self.names().clone(),
            self.children()
                .map(|field| take(&field, indices))
                .try_collect()?,
            indices.len(),
            self.validity().take(indices)?,
        )
        .map(|a| a.into_array())
    }
}

impl SliceFn for StructArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let fields = self
            .children()
            .map(|field| slice(&field, start, stop))
            .try_collect()?;
        Self::try_new(
            self.names().clone(),
            fields,
            stop - start,
            self.validity().slice(start, stop)?,
        )
        .map(|a| a.into_array())
    }
}
