use arrow_buffer::BooleanBufferBuilder;
use itertools::Itertools;
use vortex_error::{vortex_bail, VortexResult};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::SparseArray;
use crate::array::{Array, ArrayRef};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{flatten_primitive, FlattenFn, FlattenedArray};
use crate::compute::scalar_at::{scalar_at, ScalarAtFn};
use crate::compute::ArrayCompute;
use crate::match_each_native_ptype;
use crate::ptype::NativePType;
use crate::scalar::Scalar;

impl ArrayCompute for SparseArray {
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

impl AsContiguousFn for SparseArray {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
        let all_fill_types_are_equal = arrays
            .iter()
            .map(|a| a.as_sparse().fill_value())
            .all_equal();
        if !all_fill_types_are_equal {
            vortex_bail!("Cannot concatenate SparseArrays with differing fill values");
        }

        Ok(SparseArray::new(
            as_contiguous(
                &arrays
                    .iter()
                    .map(|a| a.as_sparse().indices())
                    .cloned()
                    .collect_vec(),
            )?,
            as_contiguous(
                &arrays
                    .iter()
                    .map(|a| a.as_sparse().values())
                    .cloned()
                    .collect_vec(),
            )?,
            arrays.iter().map(|a| a.len()).sum(),
            self.fill_value().clone(),
        )
        .into_array())
    }
}

impl FlattenFn for SparseArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        // Resolve our indices into a vector of usize applying the offset
        let indices = self.resolved_indices();

        let mut validity = BooleanBufferBuilder::new(self.len());
        validity.append_n(self.len(), false);
        let values = flatten_primitive(self.values())?;
        match_each_native_ptype!(values.ptype(), |$P| {
            flatten_sparse_values(
                values.typed_data::<$P>(),
                &indices,
                self.len(),
                self.fill_value(),
                validity
            )
        })
    }
}

fn flatten_sparse_values<T: NativePType>(
    values: &[T],
    indices: &[usize],
    len: usize,
    fill_value: &Scalar,
    mut validity: BooleanBufferBuilder,
) -> VortexResult<FlattenedArray> {
    let primitive_fill = if fill_value.is_null() {
        T::default()
    } else {
        fill_value.try_into()?
    };
    let mut result = vec![primitive_fill; len];

    for (v, idx) in values.iter().zip_eq(indices) {
        result[*idx] = *v;
        validity.set_bit(*idx, true);
    }

    let validity = validity.finish();
    let array = if fill_value.is_null() {
        PrimitiveArray::from_nullable(result, Some(validity.into()))
    } else {
        PrimitiveArray::from(result)
    };
    Ok(FlattenedArray::Primitive(array))
}

impl ScalarAtFn for SparseArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match self.find_index(index)? {
            None => self.fill_value().clone().cast(self.dtype()),
            Some(idx) => scalar_at(self.values(), idx)?.cast(self.dtype()),
        }
    }
}
