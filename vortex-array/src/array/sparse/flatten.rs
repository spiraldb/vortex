use arrow_buffer::BooleanBufferBuilder;
use itertools::Itertools;
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::SparseArray;
use crate::ptype::NativePType;
use crate::scalar::Scalar;
use crate::validity::Validity;
use crate::{match_each_native_ptype, ArrayFlatten, Flattened};

impl ArrayFlatten for SparseArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        // Resolve our indices into a vector of usize applying the offset
        let indices = self.resolved_indices();

        let mut validity = BooleanBufferBuilder::new(self.len());
        validity.append_n(self.len(), false);
        let values = self.values().flatten_primitive()?;
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
) -> VortexResult<Flattened<'static>> {
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
    Ok(Flattened::Primitive(array))
}
