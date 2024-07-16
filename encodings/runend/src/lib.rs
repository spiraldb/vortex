pub use default::*;
use vortex::array::bool::{Bool, BoolArray};
// pub use bool::*;
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::{Array, ArrayDef, IntoArray, ToArray};
use vortex_error::{vortex_bail, VortexResult};

use crate::bool::compress::runend_bool_encode;
use crate::bool::RunEndBoolArray;
use crate::compress::runend_primitive_encode;

mod bool;
mod default;

pub fn encode(array: Array) -> VortexResult<Array> {
    if array.encoding().id() == Primitive::ID {
        let primitive = PrimitiveArray::try_from(array)?;
        let (ends, values) = runend_primitive_encode(&primitive);
        RunEndArray::try_new(ends.into_array(), values.into_array(), primitive.validity())
            .map(|it| it.to_array())
    } else if array.encoding().id() == Bool::ID {
        let bool = BoolArray::try_from(&array)?;
        let (ends, start) = runend_bool_encode(&bool);
        RunEndBoolArray::try_new(ends.into_array(), start, bool.validity()).map(|it| it.to_array())
    } else {
        vortex_bail!("REE can only encode primitive arrays")
    }
}

#[cfg(test)]
mod test {
    use vortex::array::bool::BoolArray;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::take::take;
    use vortex::{IntoCanonical, ToArray};

    use crate::encode;

    #[test]
    fn ree_take_int() {
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

    #[test]
    fn ree_enc_dec_bool() {
        let values = vec![true, true, false, false, true];
        let ree = encode(BoolArray::from(values.clone()).to_array()).unwrap();
        assert_eq!(
            ree.into_canonical()
                .unwrap()
                .into_bool()
                .unwrap()
                .boolean_buffer()
                .iter()
                .collect::<Vec<_>>(),
            values
        );
    }
}
