pub use default::*;
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::{Array, ArrayDef, IntoArray, ToArray};
use vortex_error::{vortex_bail, VortexResult};

use crate::compress::runend_primitive_encode;

mod default;

pub fn encode(array: Array) -> VortexResult<Array> {
    if array.encoding().id() == Primitive::ID {
        let primitive = PrimitiveArray::try_from(array)?;
        let (ends, values) = runend_primitive_encode(&primitive);
        RunEndArray::try_new(ends.into_array(), values.into_array(), primitive.validity())
            .map(|it| it.to_array())
    } else {
        vortex_bail!("REE can only encode primitive arrays")
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::take::take;
    use vortex::{IntoCanonical, ToArray};

    use crate::encode;

    #[test]
    fn ree_take() {
        let ree = encode(PrimitiveArray::from(vec![1, 1, 1, 4, 4, 4, 2, 2, 5, 5, 5, 5]).to_array())
            .unwrap();
        let taken = take(&ree, PrimitiveArray::from(vec![9, 8, 1, 3]).array()).unwrap();
        assert_eq!(
            taken
                .into_canonical()
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &[5, 5, 1, 4]
        );
    }
}
