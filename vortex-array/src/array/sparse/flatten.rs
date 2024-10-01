use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, MutableBuffer};
use itertools::Itertools;
use vortex_dtype::{match_each_native_ptype, DType, NativePType};
use vortex_error::{VortexError, VortexResult};
use vortex_scalar::ScalarValue;

use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::SparseArray;
use crate::array::BoolArray;
use crate::validity::Validity;
use crate::{ArrayDType, Canonical, IntoArrayVariant, IntoCanonical};

impl IntoCanonical for SparseArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        // Resolve our indices into a vector of usize applying the offset
        let indices = self.resolved_indices();

        let validity_buffer =
            BooleanBufferBuilder::new_from_buffer(MutableBuffer::new_null(self.len()), self.len());

        if matches!(self.dtype(), DType::Bool(_)) {
            let values = self.values().into_bool()?.boolean_buffer();
            canonicalize_sparse_bools(
                values,
                &indices,
                self.len(),
                self.fill_value(),
                validity_buffer,
            )
        } else {
            let values = self.values().into_primitive()?;
            match_each_native_ptype!(values.ptype(), |$P| {
                canonicalize_sparse_primitives(
                    values.maybe_null_slice::<$P>(),
                    &indices,
                    self.len(),
                    self.fill_value(),
                    validity_buffer
                )
            })
        }
    }
}

fn canonicalize_sparse_bools(
    values: BooleanBuffer,
    indices: &[usize],
    len: usize,
    fill_value: &ScalarValue,
    mut validity_buffer: BooleanBufferBuilder,
) -> VortexResult<Canonical> {
    let fill_bool: bool = if fill_value.is_null() {
        bool::default()
    } else {
        fill_value.try_into()?
    };
    let mut flat_bools = vec![fill_bool; len];
    for (i, idx) in indices.iter().enumerate() {
        flat_bools[*idx] = values.value(i);
        validity_buffer.set_bit(*idx, true);
    }

    let validity = Validity::from(validity_buffer.finish());
    let bool_values = BoolArray::from_vec(flat_bools, validity);

    Ok(Canonical::Bool(bool_values))
}

fn canonicalize_sparse_primitives<
    T: NativePType + for<'a> TryFrom<&'a ScalarValue, Error = VortexError>,
>(
    values: &[T],
    indices: &[usize],
    len: usize,
    fill_value: &ScalarValue,
    mut validity: BooleanBufferBuilder,
) -> VortexResult<Canonical> {
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
        PrimitiveArray::from_vec(result, Validity::from(validity))
    } else {
        PrimitiveArray::from(result)
    };
    Ok(Canonical::Primitive(array))
}

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, Nullability};

    use crate::array::sparse::SparseArray;
    use crate::array::BoolArray;
    use crate::validity::Validity;
    use crate::{ArrayDType, Canonical, IntoArray, IntoCanonical};

    #[test]
    fn test_sparse_bool() {
        let indices = vec![0u64].into_array();
        let values = BoolArray::from_vec(vec![true], Validity::NonNullable).into_array();
        let sparse_bools = SparseArray::try_new(indices, values, 10, true.into()).unwrap();
        assert_eq!(*sparse_bools.dtype(), DType::Bool(Nullability::NonNullable));
        let flat_bools = sparse_bools.into_canonical().unwrap();
        assert!(matches!(flat_bools, Canonical::Bool(_)));
    }
}
