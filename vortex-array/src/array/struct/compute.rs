use std::sync::Arc;

use arrow_array::{
    Array as ArrowArray, ArrayRef as ArrowArrayRef, StructArray as ArrowStructArray,
};
use arrow_schema::{Field, Fields};
use itertools::Itertools;
use vortex_error::VortexResult;

use crate::array::r#struct::StructArray;
use crate::compute::as_arrow::{as_arrow, AsArrowArray};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::slice::{slice, SliceFn};
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
use crate::scalar::{Scalar, StructScalar};
use crate::{Array, ArrayDType, IntoArray, IntoArrayData, OwnedArray};
use crate::{ArrayTrait, ToStatic};

impl ArrayCompute for StructArray<'_> {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
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

impl AsArrowArray for StructArray<'_> {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        let field_arrays: Vec<ArrowArrayRef> =
            self.children().map(|f| as_arrow(&f)).try_collect()?;

        let arrow_fields: Fields = self
            .names()
            .iter()
            .zip(field_arrays.iter())
            .zip(self.fields().iter())
            .map(|((name, arrow_field), vortex_field)| {
                Field::new(
                    name.as_str(),
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

impl AsContiguousFn for StructArray<'_> {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<OwnedArray> {
        let mut fields = vec![Vec::new(); self.fields().len()];
        for array in arrays {
            let struct_arr = StructArray::try_from(array).unwrap();
            for f in 0..self.fields().len() {
                fields[f].push(struct_arr.child(f).unwrap().to_static())
            }
        }

        StructArray::try_new(
            self.names().clone(),
            fields
                .iter()
                .map(|field_arrays| as_contiguous(field_arrays).map(|a| a.into_array_data()))
                .try_collect()?,
            self.len(),
        )
        .map(|a| a.into_array())
    }
}

impl ScalarAtFn for StructArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(StructScalar::new(
            self.dtype().clone(),
            self.children()
                .map(|field| scalar_at(&field, index))
                .try_collect()?,
        )
        .into())
    }
}

impl TakeFn for StructArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        StructArray::try_new(
            self.names().clone(),
            self.children()
                .map(|field| take(&field, indices).map(|a| a.into_array_data()))
                .try_collect()?,
            indices.len(),
        )
        .map(|a| a.into_array())
    }
}

impl SliceFn for StructArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        let fields = self
            .children()
            .map(|field| slice(&field, start, stop).map(|a| a.into_array_data()))
            .try_collect()?;
        StructArray::try_new(self.names().clone(), fields, stop - start).map(|a| a.into_array())
    }
}
