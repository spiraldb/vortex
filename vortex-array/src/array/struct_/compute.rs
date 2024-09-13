use itertools::Itertools;
use vortex_error::{vortex_err, VortexResult};
use vortex_scalar::Scalar;

use crate::array::struct_::StructArray;
use crate::compute::unary::{scalar_at, scalar_at_unchecked, ScalarAtFn};
use crate::compute::{filter, slice, take, ArrayCompute, FilterFn, SliceFn, TakeFn};
use crate::variants::StructArrayTrait;
use crate::{Array, ArrayDType, IntoArray};

impl ArrayCompute for StructArray {
    fn filter(&self) -> Option<&dyn FilterFn> {
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

impl ScalarAtFn for StructArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(Scalar::r#struct(
            self.dtype().clone(),
            self.children()
                .map(|field| scalar_at(&field, index).map(|s| s.into_value()))
                .try_collect()?,
        ))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        Scalar::r#struct(
            self.dtype().clone(),
            self.children()
                .map(|field| scalar_at_unchecked(&field, index).into_value())
                .collect(),
        )
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

impl FilterFn for StructArray {
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        let fields: Vec<Array> = self
            .children()
            .map(|field| filter(&field, predicate))
            .try_collect()?;
        let length = fields
            .first()
            .map(|a| a.len())
            .ok_or_else(|| vortex_err!("Struct arrays should have at least one field"))?;

        Self::try_new(
            self.names().clone(),
            fields,
            length,
            self.validity().filter(predicate)?,
        )
        .map(|a| a.into_array())
    }
}
