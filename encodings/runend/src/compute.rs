use vortex::array::{BoolArray, ConstantArray, PrimitiveArray, SparseArray};
use vortex::compute::unary::{scalar_at, scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{filter, slice, take, ArrayCompute, SliceFn, TakeFn};
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::RunEndArray;

impl ArrayCompute for RunEndArray {
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

impl ScalarAtFn for RunEndArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        scalar_at(&self.values(), self.find_physical_index(index)?)
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        let idx = self
            .find_physical_index(index)
            .expect("Search must be implemented for the underlying index array");
        scalar_at_unchecked(&self.values(), idx)
    }
}

impl TakeFn for RunEndArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let primitive_indices = indices.clone().into_primitive()?;
        let physical_indices = match_each_integer_ptype!(primitive_indices.ptype(), |$P| {
            primitive_indices
                .maybe_null_slice::<$P>()
                .iter()
                .map(|idx| *idx as usize)
                .map(|idx| {
                    if idx >= self.len() {
                        vortex_bail!(OutOfBounds: idx, 0, self.len())
                    }
                    self.find_physical_index(idx).map(|loc| loc as u64)
                })

                .collect::<VortexResult<Vec<_>>>()?
        });
        let physical_indices_array = PrimitiveArray::from(physical_indices).into_array();
        let dense_values = take(&self.values(), &physical_indices_array)?;

        Ok(match self.validity() {
            Validity::NonNullable => dense_values,
            Validity::AllValid => dense_values,
            Validity::AllInvalid => {
                ConstantArray::new(Scalar::null(self.dtype().clone()), indices.len()).into_array()
            }
            Validity::Array(original_validity) => {
                let dense_validity = take(&original_validity, indices)?;
                let dense_nonnull_indices = PrimitiveArray::from(
                    BoolArray::try_from(dense_validity.clone())?
                        .boolean_buffer()
                        .set_indices()
                        .map(|idx| idx as u64)
                        .collect::<Vec<u64>>(),
                )
                .into_array();
                let length = dense_validity.len();

                SparseArray::try_new(
                    dense_nonnull_indices,
                    filter(&dense_values, &dense_validity)?,
                    length,
                    Scalar::null(self.dtype().clone()),
                )?
                .into_array()
            }
        })
    }
}

impl SliceFn for RunEndArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let slice_begin = self.find_physical_index(start)?;
        let slice_end = self.find_physical_index(stop)?;

        Ok(Self::with_offset_and_size(
            slice(&self.ends(), slice_begin, slice_end + 1)?,
            slice(&self.values(), slice_begin, slice_end + 1)?,
            self.validity().slice(slice_begin, slice_end + 1)?,
            stop - start,
            start,
        )?
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::PrimitiveArray;
    use vortex::compute::take;
    use vortex::compute::unary::{scalar_at, try_cast};
    use vortex::validity::Validity;
    use vortex::{ArrayDType, IntoArrayVariant, ToArray};
    use vortex_scalar::Scalar;

    use crate::RunEndArray;

    fn ree_array() -> RunEndArray {
        RunEndArray::encode(
            PrimitiveArray::from(vec![1, 1, 1, 4, 4, 4, 2, 2, 5, 5, 5, 5]).to_array(),
        )
        .unwrap()
    }

    #[test]
    fn ree_take() {
        let taken = take(
            ree_array().array(),
            PrimitiveArray::from(vec![9, 8, 1, 3]).array(),
        )
        .unwrap();
        assert_eq!(
            taken.into_primitive().unwrap().maybe_null_slice::<i32>(),
            &[5, 5, 1, 4]
        );
    }

    #[test]
    fn ree_take_end() {
        let taken = take(ree_array().array(), PrimitiveArray::from(vec![11]).array()).unwrap();
        assert_eq!(
            taken.into_primitive().unwrap().maybe_null_slice::<i32>(),
            &[5]
        );
    }

    #[test]
    #[should_panic]
    fn ree_take_out_of_bounds() {
        take(ree_array().array(), PrimitiveArray::from(vec![12]).array()).unwrap();
    }

    #[test]
    fn ree_scalar_at_end() {
        let scalar = scalar_at(ree_array().array(), 11).unwrap();
        assert_eq!(scalar, 5.into());
    }

    #[test]
    fn ree_null_scalar() {
        let array = ree_array();
        let null_ree = RunEndArray::try_new(
            array.ends().clone(),
            try_cast(&array.values(), &array.values().dtype().as_nullable()).unwrap(),
            Validity::AllInvalid,
        )
        .unwrap();
        let scalar = scalar_at(null_ree.array(), 11).unwrap();
        assert_eq!(scalar, Scalar::null(null_ree.dtype().clone()));
    }
}
