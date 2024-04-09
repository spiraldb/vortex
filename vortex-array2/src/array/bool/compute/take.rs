use arrow_buffer::BooleanBuffer;
use num_traits::AsPrimitive;
use vortex::match_each_integer_ptype;
use vortex_error::VortexResult;

use crate::array::bool::{BoolArray, BoolData};
use crate::compute::flatten::flatten_primitive;
use crate::compute::take::TakeFn;
use crate::IntoArray;
use crate::{Array, OwnedArray};

impl TakeFn for BoolArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        let validity = self.validity().take(indices)?;
        let indices_data = flatten_primitive(indices)?;
        let indices = indices_data.to_typed_array();
        match_each_integer_ptype!(indices.ptype(), |$I| {
            Ok(BoolData::from_vec(
                take_bool(&self.buffer(), indices.typed_data::<$I>()),
                validity,
            ).into_array())
        })
    }
}

fn take_bool<I: AsPrimitive<usize>>(bools: &BooleanBuffer, indices: &[I]) -> Vec<bool> {
    indices.iter().map(|&idx| bools.value(idx.as_())).collect()
}

#[cfg(test)]
mod test {
    use crate::array::bool::{BoolData, BoolDef};
    use crate::array::primitive::PrimitiveData;
    use crate::compute::take::take;
    use crate::validity::Validity::NonNullable;
    use crate::IntoArray;

    #[test]
    fn take_nullable() {
        let reference = BoolData::from_iter(vec![
            Some(false),
            Some(true),
            Some(false),
            None,
            Some(false),
        ])
        .into_array();

        let res = take(
            &reference,
            &PrimitiveData::from_vec(vec![0, 3, 4], NonNullable).into_array(),
        )
        .unwrap()
        .to_typed_array::<BoolDef>()
        .unwrap();

        assert_eq!(
            res.buffer(),
            BoolData::from_iter(vec![Some(false), None, Some(false)])
                .to_typed_array()
                .buffer()
        );
    }
}
