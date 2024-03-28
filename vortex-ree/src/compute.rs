use vortex::array::primitive::PrimitiveArray;
use vortex::array::{Array, ArrayRef};
use vortex::compute::flatten::{flatten, flatten_primitive, FlattenFn, FlattenedArray};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::match_each_integer_ptype;
use vortex::scalar::Scalar;
use vortex::validity::ArrayValidity;
use vortex_error::{VortexError, VortexResult};

use crate::compress::ree_decode;
use crate::REEArray;

impl ArrayCompute for REEArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl FlattenFn for REEArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        let ends = flatten(self.ends())?;
        let FlattenedArray::Primitive(pends) = ends else {
            return Err(VortexError::InvalidArgument(
                "REE Ends array didn't flatten to primitive".into(),
            ));
        };

        let values = flatten(self.values())?;
        if let FlattenedArray::Primitive(pvalues) = values {
            ree_decode(&pends, &pvalues, self.validity(), self.offset(), self.len())
                .map(FlattenedArray::Primitive)
        } else {
            Err(VortexError::InvalidArgument(
                "Cannot yet flatten non-primitive REE array".into(),
            ))
        }
    }
}

impl ScalarAtFn for REEArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        scalar_at(self.values(), self.find_physical_index(index)?)
    }
}

impl TakeFn for REEArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let primitive_indices = flatten_primitive(indices)?;
        let mut values_to_take: Vec<u64> = Vec::new();
        let physical_indices: Vec<u64> = match_each_integer_ptype!(primitive_indices.ptype(), |$P| {
            primitive_indices
                .typed_data::<$P>()
                .iter()
                .map(|idx| {
                    self.find_physical_index(*idx as usize).map(|loc| {
                        values_to_take
                            .iter()
                            .position(|to_take| *to_take == loc as u64)
                            .map(|p| p as u64)
                            .unwrap_or_else(|| {
                                let position = values_to_take.len();
                                values_to_take.push(loc as u64);
                                position as u64
                            })
                    })
                })
                .collect::<VortexResult<Vec<_>>>()?
        });
        let taken_values = take(self.values(), &PrimitiveArray::from(values_to_take))?;
        take(&taken_values, &PrimitiveArray::from(physical_indices))
    }
}

#[cfg(test)]
mod test {
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::take::take;

    use crate::REEArray;

    #[test]
    fn ree_take() {
        let ree = REEArray::encode(&PrimitiveArray::from(vec![
            1, 1, 1, 4, 4, 4, 2, 2, 5, 5, 5, 5,
        ]))
        .unwrap();
        let taken = take(&ree, &PrimitiveArray::from(vec![8, 1, 3])).unwrap();
        assert_eq!(taken.as_primitive().typed_data::<i32>(), &[5, 1, 4]);
    }
}
