use arrow_buffer::BooleanBufferBuilder;
use itertools::Itertools;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::SparseArray;
use crate::array::{Array, ArrayRef, OwnedArray};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{flatten, FlattenFn, FlattenedArray};
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
        let values = flatten(self.values())?;
        let null_fill = self.fill_value().is_null();
        if let FlattenedArray::Primitive(ref parray) = values {
            match_each_native_ptype!(parray.ptype(), |$P| {
                flatten_primitive::<$P>(
                    self,
                    parray,
                    indices,
                    null_fill,
                    validity
                )
            })
        } else {
            Err(vortex_err!(
                "Cannot flatten SparseArray with non-primitive values"
            ))
        }
    }
}
fn flatten_primitive<T: NativePType>(
    sparse_array: &SparseArray,
    parray: &PrimitiveArray,
    indices: Vec<usize>,
    null_fill: bool,
    mut validity: BooleanBufferBuilder,
) -> VortexResult<FlattenedArray> {
    let fill_value = if null_fill {
        T::default()
    } else {
        sparse_array.fill_value.clone().try_into()?
    };
    let mut values = vec![fill_value; sparse_array.len()];

    for (offset, v) in parray.typed_data::<T>().iter().enumerate() {
        let idx = indices[offset];
        values[idx] = *v;
        validity.set_bit(idx, true);
    }

    let validity = validity.finish();
    if null_fill {
        Ok(FlattenedArray::Primitive(PrimitiveArray::from_nullable(
            values,
            Some(validity.into()),
        )))
    } else {
        Ok(FlattenedArray::Primitive(PrimitiveArray::from(values)))
    }
}

impl ScalarAtFn for SparseArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        match self.find_index(index)? {
            None => self.fill_value().clone().cast(self.dtype()),
            Some(idx) => scalar_at(self.values(), idx)?.cast(self.dtype()),
        }
    }
}
