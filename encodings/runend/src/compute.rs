use vortex::array::{ConstantArray, PrimitiveArray, SparseArray};
use vortex::compute::unary::{scalar_at, scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{filter, slice, take, ArrayCompute, SliceFn, TakeFn};
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::{vortex_bail, VortexExpect as _, VortexResult};
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
            .vortex_expect("Search must be implemented for the underlying index array");
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
                    dense_validity
                        .clone()
                        .into_bool()?
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
            self.validity().slice(start, stop)?,
            stop - start,
            start,
        )?
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::{BoolArray, PrimitiveArray};
    use vortex::compute::unary::{scalar_at, try_cast};
    use vortex::compute::{slice, take};
    use vortex::validity::{ArrayValidity, Validity};
    use vortex::{ArrayDType, IntoArray, IntoArrayVariant, ToArray};
    use vortex_dtype::{DType, Nullability, PType};
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

    #[test]
    fn slice_with_nulls() {
        let array = RunEndArray::try_new(
            PrimitiveArray::from(vec![3, 6, 8, 12]).into_array(),
            PrimitiveArray::from_vec(vec![1, 4, 2, 5], Validity::AllValid).into_array(),
            Validity::from(vec![
                false, false, false, false, true, true, false, false, false, false, true, true,
            ]),
        )
        .unwrap();
        let sliced = slice(array.array(), 4, 10).unwrap();
        let sliced_primitive = sliced.into_primitive().unwrap();
        assert_eq!(
            sliced_primitive.maybe_null_slice::<i32>(),
            vec![4, 4, 2, 2, 5, 5]
        );
        assert_eq!(
            sliced_primitive
                .logical_validity()
                .into_array()
                .into_bool()
                .unwrap()
                .boolean_buffer()
                .iter()
                .collect::<Vec<_>>(),
            vec![true, true, false, false, false, false]
        )
    }

    #[test]
    fn slice_array() {
        let arr = slice(
            RunEndArray::try_new(
                vec![2u32, 5, 10].into_array(),
                vec![1i32, 2, 3].into_array(),
                Validity::NonNullable,
            )
            .unwrap()
            .array(),
            3,
            8,
        )
        .unwrap();
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );
        assert_eq!(arr.len(), 5);

        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            vec![2, 2, 3, 3, 3]
        );
    }

    #[test]
    fn slice_end_inclusive() {
        let arr = slice(
            RunEndArray::try_new(
                vec![2u32, 5, 10].into_array(),
                vec![1i32, 2, 3].into_array(),
                Validity::NonNullable,
            )
            .unwrap()
            .array(),
            4,
            10,
        )
        .unwrap();
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );
        assert_eq!(arr.len(), 6);

        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            vec![2, 3, 3, 3, 3, 3]
        );
    }

    #[test]
    fn decompress() {
        let arr = RunEndArray::try_new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            Validity::NonNullable,
        )
        .unwrap();

        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            vec![1, 1, 2, 2, 2, 3, 3, 3, 3, 3]
        );
    }

    #[test]
    fn take_with_nulls() {
        let uncompressed = PrimitiveArray::from_vec(vec![1i32, 0, 3], Validity::AllValid);
        let validity = BoolArray::from_vec(
            vec![
                true, true, false, false, false, true, true, true, true, true,
            ],
            Validity::NonNullable,
        );
        let arr = RunEndArray::try_new(
            vec![2u32, 5, 10].into_array(),
            uncompressed.into(),
            Validity::Array(validity.into()),
        )
        .unwrap();

        let test_indices = PrimitiveArray::from_vec(vec![0, 2, 4, 6], Validity::NonNullable);
        let taken = take(arr.array(), test_indices.array()).unwrap();

        assert_eq!(taken.len(), test_indices.len());

        let parray = taken.into_primitive().unwrap();
        assert_eq!(
            (0..4)
                .map(|idx| parray.is_valid(idx).then(|| parray.get_as_cast::<i32>(idx)))
                .collect::<Vec<Option<i32>>>(),
            vec![Some(1), None, None, Some(3),]
        );
    }
}
