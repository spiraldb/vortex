use arrow_buffer::BooleanBuffer;
use num_traits::AsPrimitive;
use vortex::match_each_integer_ptype;
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::compute::take::TakeFn;
use crate::IntoArray;
use crate::{Array, OwnedArray};

impl TakeFn for BoolArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        let validity = self.validity();
        let indices = indices.clone().flatten_primitive()?;
        match_each_integer_ptype!(indices.ptype(), |$I| {
            Ok(BoolArray::from_vec(
                take_bool(&self.boolean_buffer(), indices.typed_data::<$I>()),
                validity.take(indices.array())?,
            ).into_array())
        })
    }
}

fn take_bool<I: AsPrimitive<usize>>(bools: &BooleanBuffer, indices: &[I]) -> Vec<bool> {
    indices.iter().map(|&idx| bools.value(idx.as_())).collect()
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::array::primitive::PrimitiveArray;
    use crate::compute::take::take;
    use crate::IntoArray;

    #[test]
    fn take_nullable() {
        let reference = BoolArray::from_iter(vec![
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(false),
        ])
        .into_array();

        let b = BoolArray::try_from(
            take(
                &reference,
                &PrimitiveArray::from(vec![0, 3, 4]).into_array(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            b.boolean_buffer(),
            BoolArray::from_iter(vec![Some(false), None, Some(false)]).boolean_buffer()
        );
    }
}
